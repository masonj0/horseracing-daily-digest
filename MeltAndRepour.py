#!/usr/bin/env python3
"""
Melt & Repour Global Racing Scanner V2.2

- Async HTTP with reusable client, concurrency control, optional HTTP/2, and ETag/If-Modified-Since caching
- Cache TTL respects Cache-Control max-age when present
- Multi-source architecture with provenance (ATR + GB Greyhounds + AU Harness + CA Harness)
- Value scoring and ranked report
- Configurable thresholds via CLI
- UTC normalization with local time/tz output
- Deduplication with composite key and field-level merge
- Optional headless browser fallback (undetected-chromedriver), toggle via --disable-browser-fetch or DISABLE_BROWSER_FETCH=1
- Outputs: HTML + JSON + CSV
"""

import asyncio
import argparse
import csv
import json
import logging
import os
import re
import hashlib
import random
import contextlib
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from zoneinfo import ZoneInfo
import time

import httpx
import aiofiles
from bs4 import BeautifulSoup

# Silence urllib3 warnings when user disables SSL verification
import urllib3

SCHEMA_VERSION = "2.2"
DEFAULT_CACHE_DIR = Path(".cache")
DEFAULT_OUTPUT_DIR = Path("output")
REQUEST_TIMEOUT = 30.0

# Track timezone mapping (seed data; grow over time)
TRACK_TIMEZONES = {
    # UK & Ireland
    "ascot": "Europe/London",
    "cheltenham": "Europe/London",
    "newmarket": "Europe/London",
    "leopardstown": "Europe/Dublin",
    "curragh": "Europe/Dublin",
    # USA (examples)
    "churchill": "America/New_York",
    "belmont": "America/New_York",
    "saratoga": "America/New_York",
    "santa-anita": "America/Los_Angeles",
    "del-mar": "America/Los_Angeles",
    # Australia
    "flemington": "Australia/Melbourne",
    "randwick": "Australia/Sydney",
    "eagle-farm": "Australia/Brisbane",
    # France, HK, Japan
    "longchamp": "Europe/Paris",
    "sha-tin": "Asia/Hong_Kong",
    "tokyo": "Asia/Tokyo",
}

COUNTRY_TIMEZONES = {
    "GB": "Europe/London",
    "IE": "Europe/Dublin",
    "US": "America/New_York",
    "AU": "Australia/Sydney",
    "NZ": "Pacific/Auckland",
    "FR": "Europe/Paris",
    "HK": "Asia/Hong_Kong",
    "JP": "Asia/Tokyo",
    "ZA": "Africa/Johannesburg",
    "CA": "America/Toronto",  # default for Canada; refine by track later
}

# Extend track-specific timezones (AU harness)
TRACK_TIMEZONES.update({
    "albion-park": "Australia/Brisbane",   # QLD (no DST)
    "redcliffe": "Australia/Brisbane",
    "menangle": "Australia/Sydney",        # NSW
    "gloucester-park": "Australia/Perth",  # WA
})


def resolve_chrome_binary() -> Optional[str]:
    import shutil
    for p in [
        os.getenv("GOOGLE_CHROME_BIN"),
        os.getenv("CHROME_BIN"),
        "/usr/bin/google-chrome-stable",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        r"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
        r"C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
    ]:
        if p and os.path.exists(p):
            return p
    return (
        shutil.which("google-chrome")
        or shutil.which("google-chrome-stable")
        or shutil.which("chromium")
        or None
    )


def convert_odds_to_fractional(odds_str: str) -> float:
    if not isinstance(odds_str, str) or not odds_str.strip():
        return 999.0
    s = odds_str.strip().upper().replace("-", "/")
    if s in {"SP", "NR"}:
        return 999.0
    if s in {"EVS", "EVENS"}:
        return 1.0
    if "/" in s:
        try:
            num, den = map(float, s.split("/", 1))
            return num / den if den > 0 else 999.0
        except Exception:
            return 999.0
    try:
        dec = float(s)
        return dec - 1.0 if dec > 1 else 999.0
    except Exception:
        return 999.0


# ---- time helpers ----
def parse_local_hhmm(time_text: str) -> Optional[str]:
    """
    Parse 'H:MM' or 'HH:MM' optionally followed by AM/PM (case-insensitive),
    returning 24-hour 'HH:MM'. Returns None if no time found.
    """
    if not time_text:
        return None
    m = re.search(r"\b(\d{1,2}):(\d{2})\s*([AaPp][Mm])?\b", time_text)
    if not m:
        return None
    h, mm, ap = m.group(1), m.group(2), (m.group(3) or "").upper()
    hour = int(h)
    if ap == "AM":
        hour = 0 if hour == 12 else hour
    elif ap == "PM":
        hour = 12 if hour == 12 else hour + 12
    hour = max(0, min(23, hour))
    return f"{hour:02d}:{mm}"


@dataclass
class RaceData:
    id: str
    course: str
    race_time: str
    utc_datetime: datetime
    local_time: str
    timezone_name: str
    field_size: int
    country: str
    discipline: str  # 'thoroughbred' | 'greyhound' | 'harness'
    race_number: Optional[int]
    grade: Optional[str]
    distance: Optional[str]
    surface: Optional[str]
    favorite: Optional[Dict[str, Any]]
    second_favorite: Optional[Dict[str, Any]]
    all_runners: List[Dict[str, Any]]
    race_url: str
    form_guide_url: Optional[str] = None
    value_score: float = 0.0
    data_sources: Dict[str, str] = field(default_factory=dict)


class CacheManager:
    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(exist_ok=True, parents=True)
        self.metadata_file = cache_dir / "metadata.json"
        self.metadata = self._load_metadata_sync()
        self.hit_count = 0
        self.miss_count = 0

    def _load_metadata_sync(self) -> Dict[str, Any]:
        if self.metadata_file.exists():
            try:
                return json.loads(self.metadata_file.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    async def _save_metadata(self):
        async with aiofiles.open(self.metadata_file, "w", encoding="utf-8") as f:
            await f.write(json.dumps(self.metadata, indent=2))

    def _cache_key(self, url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()[:24]

    async def get(self, url: str) -> Optional[Tuple[str, Dict[str, str]]]:
        key = self._cache_key(url)
        cache_file = self.cache_dir / f"{key}.html"
        meta = self.metadata.get(key, {})
        now_ts = datetime.now().timestamp()
        if not cache_file.exists():
            self.miss_count += 1
            return None
        if "expires" in meta and now_ts > float(meta["expires"]):
            with contextlib.suppress(Exception):
                cache_file.unlink(missing_ok=True)
            self.miss_count += 1
            return None
        try:
            async with aiofiles.open(cache_file, "r", encoding="utf-8") as f:
                content = await f.read()
            self.hit_count += 1
            return (content, meta.get("headers", {}))
        except Exception:
            self.miss_count += 1
            return None

    def _ttl_from_headers(self, headers: Dict[str, str], default_ttl: int = 1800) -> int:
        cc = headers.get("cache-control") or headers.get("Cache-Control")
        if cc:
            m = re.search(r"max-age=(\d+)", cc)
            if m:
                try:
                    return max(60, min(6 * 3600, int(m.group(1))))
                except Exception:
                    pass
        return default_ttl

    async def set(self, url: str, content: str, headers: Dict[str, str]):
        key = self._cache_key(url)
        cache_file = self.cache_dir / f"{key}.html"
        ttl_seconds = self._ttl_from_headers(headers)
        async with aiofiles.open(cache_file, "w", encoding="utf-8") as f:
            await f.write(content)
        self.metadata[key] = {
            "url": url,
            "cached_at": datetime.now().isoformat(),
            "expires": datetime.now().timestamp() + ttl_seconds,
            "headers": {
                k.lower(): v for k, v in dict(headers).items()
                if k and k.lower() in {"etag", "last-modified", "content-type", "cache-control"}
            }
        }
        await self._save_metadata()


class AsyncHttpClient:
    def __init__(self, max_concurrent: int, cache_manager: Optional[CacheManager], verify_ssl: bool = True, http2: bool = True):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.cache_manager = cache_manager
        self.limits = httpx.Limits(max_connections=40, max_keepalive_connections=10)
        self.retry_count = 0
        self.blocked_count = 0
        self.success_count = 0
        self.browser_success = 0
        self.browser_attempts = 0
        self.ua_pool = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        ]
        self._ua_idx = 0
        self._client: Optional[httpx.AsyncClient] = None
        self.verify_ssl = verify_ssl
        self.http2 = http2
        # per-host throttle
        self._host_last: Dict[str, float] = {}
        self._throttle_lock = asyncio.Lock()
        self.min_interval_per_host = 0.25  # seconds

    def _ua(self) -> str:
        ua = self.ua_pool[self._ua_idx % len(self.ua_pool)]
        self._ua_idx += 1
        return ua

    async def _client_get(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                limits=self.limits,
                timeout=REQUEST_TIMEOUT,
                follow_redirects=True,
                verify=self.verify_ssl,
                http2=self.http2,
                headers={
                    "Accept": "*/*",
                    "Accept-Language": "en-US,en;q=0.8",
                    "Connection": "keep-alive",
                }
            )
        return self._client

    async def aclose(self):
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _throttle(self, url: str):
        try:
            host = httpx.URL(url).host
        except Exception:
            return
        async with self._throttle_lock:
            now = time.perf_counter()
            last = self._host_last.get(host, 0.0)
            wait = self.min_interval_per_host - (now - last)
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.perf_counter()
            self._host_last[host] = now

    async def fetch(self, url: str, use_browser: bool = False) -> Optional[str]:
        async with self.semaphore:
            if self.cache_manager:
                cached = await self.cache_manager.get(url)
                if cached:
                    return cached[0]

            content = await self._fetch_http(url)
            if content:
                self.success_count += 1
                return content

            if use_browser and os.getenv("DISABLE_BROWSER_FETCH") != "1":
                self.browser_attempts += 1
                content = await self._fetch_browser(url)
                if content:
                    self.browser_success += 1
                    self.success_count += 1
                    if self.cache_manager:
                        await self.cache_manager.set(url, content, headers={"cache-control": "max-age=600"})
                    return content

            self.blocked_count += 1
            return None

    async def _fetch_http(self, url: str) -> Optional[str]:
        await self._throttle(url)
        headers = {"User-Agent": self._ua()}
        if self.cache_manager:
            cached = await self.cache_manager.get(url)
            if cached and cached[1]:
                h = cached[1]
                if "etag" in h:
                    headers["If-None-Match"] = h["etag"]
                if "last-modified" in h:
                    headers["If-Modified-Since"] = h["last-modified"]

        client = await self._client_get()
        for attempt in range(4):
            try:
                r = await client.get(url, headers=headers)
                if r.status_code == 304 and self.cache_manager:
                    cached = await self.cache_manager.get(url)
                    if cached:
                        return cached[0]
                r.raise_for_status()
                text = r.text
                if self.cache_manager and r.status_code == 200:
                    await self.cache_manager.set(url, text, r.headers)
                return text
            except httpx.HTTPStatusError as e:
                code = e.response.status_code
                if code in (429, 503, 502, 500, 520, 521, 522):
                    self.retry_count += 1
                    backoff = (2 ** attempt) + random.random() * 0.3
                    await asyncio.sleep(backoff)
                    continue
                logging.debug(f"HTTP status {code} for {url}")
                break
            except (httpx.TimeoutException, httpx.RequestError):
                self.retry_count += 1
                await asyncio.sleep(0.5 + random.random() * 0.25)
                continue
        return None

    async def _fetch_browser(self, url: str) -> Optional[str]:
        await self._throttle(url)
        try:
            chrome_bin = resolve_chrome_binary()
            if not chrome_bin:
                return None
            import undetected_chromedriver as uc  # lazy import
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            options = uc.ChromeOptions()
            options.binary_location = chrome_bin
            for arg in ["--headless=new", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--window-size=1920,1080"]:
                options.add_argument(arg)
            options.add_argument("user-agent=" + self._ua())

            driver = uc.Chrome(options=options)
            try:
                driver.get(url)
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                return driver.page_source or None
            finally:
                with contextlib.suppress(Exception):
                    driver.quit()
        except Exception as e:
            logging.debug(f"Browser fetch failed for {url}: {e}")
            return None


class ValueScorer:
    def __init__(self, thresholds: Dict[str, Any]):
        self.weights = {"field_size": 0.3, "odds_value": 0.4, "odds_spread": 0.2, "data_quality": 0.1}
        self.thresholds = thresholds

    def calculate_score(self, race: RaceData) -> float:
        score = 0.0
        field_score = max(0.0, (12 - max(0, race.field_size)) / 12 * 100)
        score += field_score * self.weights["field_size"]

        fav = race.favorite or {}
        sec = race.second_favorite or {}
        fav_f = convert_odds_to_fractional(fav.get("odds_str", ""))
        sec_f = convert_odds_to_fractional(sec.get("odds_str", ""))
        if fav_f != 999.0:
            fav_score = min(100.0, max(0.0, (fav_f - 0.5) / 3.5 * 100))
            score += fav_score * self.weights["odds_value"]
        if fav_f != 999.0 and sec_f != 999.0 and sec_f > fav_f:
            spread_score = min(100.0, max(0.0, (sec_f - fav_f)) / 5 * 100)
            score += spread_score * self.weights["odds_spread"]

        quality = min(100.0, len(race.data_sources) * 25.0)
        score += quality * self.weights["data_quality"]

        return max(0.0, min(100.0, score))


class DataSourceBase:
    def __init__(self, http_client: AsyncHttpClient):
        self.http_client = http_client
        self.name = self.__class__.__name__

    def _normalize_course_name(self, name: str) -> str:
        if not name:
            return ""
        return re.sub(r"\s*\([^)]*\)", "", name.lower().strip())

    def _track_tz(self, course: str, country: str) -> str:
        key = self._normalize_course_name(course).replace(" ", "-")
        return TRACK_TIMEZONES.get(key) or COUNTRY_TIMEZONES.get(country, "UTC")

    def _race_id(self, course: str, date: str, time_s: str, race_num: Optional[int]) -> str:
        parts = [self._normalize_course_name(course), date, re.sub(r"[^\d]", "", time_s or "")]
        if race_num:
            parts.append(str(race_num))
        return hashlib.sha256("|".join(parts).encode()).hexdigest()[:12]

    async def fetch_races(self, date_range: Tuple[datetime, datetime]) -> List[RaceData]:
        raise NotImplementedError


class AtTheRacesSource(DataSourceBase):
    REGIONS = ["uk", "ireland", "usa", "france", "saf", "aus"]

    async def fetch_races(self, date_range: Tuple[datetime, datetime]) -> List[RaceData]:
        out: List[RaceData] = []
        for dt in self._days(date_range):
            tasks = []
            for region in self.REGIONS:
                url = f"https://www.attheraces.com/ajax/marketmovers/tabs/{region}/{dt.strftime('%Y%m%d')}"
                tasks.append(self._fetch_region(url, region, dt))
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, list):
                    out.extend(res)
                elif isinstance(res, Exception):
                    logging.debug(f"ATR region fetch error: {res}")
        return out

    def _days(self, date_range: Tuple[datetime, datetime]):
        cur = date_range[0].replace(hour=0, minute=0, second=0, microsecond=0)
        end = date_range[1].replace(hour=0, minute=0, second=0, microsecond=0)
        while cur <= end:
            yield cur
            cur += timedelta(days=1)

    async def _fetch_region(self, url: str, region: str, dt: datetime) -> List[RaceData]:
        html = await self.http_client.fetch(url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        races: List[RaceData] = []
        for caption in soup.find_all("caption", string=re.compile(r"^\d{2}:\d{2}")):
            rd = self._parse_from_caption(caption, region, dt)
            if rd:
                races.append(rd)
        return races

    def _parse_from_caption(self, caption, region: str, dt: datetime) -> Optional[RaceData]:
        try:
            m = re.match(r"(\d{2}:\d{2})", caption.get_text(strip=True))
            if not m:
                return None
            race_time = m.group(1)

            panel = caption.find_parent("div", class_=re.compile(r"\bpanel\b")) or caption.find_parent("div")
            course_header = None
            if panel:
                course_header = panel.find("h2")
            if not course_header:
                course_header = caption.find_previous("h2")
            if not course_header:
                return None
            course_name = course_header.get_text(strip=True)

            table = caption.find_next_sibling("table") or caption.find_parent().find("table")
            if not table:
                return None
            runners: List[Dict[str, Any]] = []
            body = table.find("tbody") or table
            for row in body.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                name = cells[0].get_text(strip=True)
                odds = cells[1].get_text(strip=True)
                if name:
                    runners.append({"name": name, "odds_str": odds})
            if not runners:
                return None

            country_map = {"uk": "GB", "ireland": "IE", "usa": "US", "france": "FR", "saf": "ZA", "aus": "AU"}
            country = country_map.get(region, "GB")
            tz_name = self._track_tz(course_name, country)

            local_tz = ZoneInfo(tz_name)
            local_dt = datetime.combine(dt.date(), datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=local_tz)
            utc_dt = local_dt.astimezone(ZoneInfo("UTC"))

            course_slug = re.sub(r"\s+", "-", course_name.strip().lower())
            date_str = dt.strftime("%Y-%m-%d")
            time_slug = race_time.replace(":", "")
            race_url = f"https://www.attheraces.com/racecard/{course_slug}/{date_str}/{time_slug}"

            fav_sorted = sorted(runners, key=lambda x: convert_odds_to_fractional(x.get("odds_str", "")))
            favorite = fav_sorted[0] if fav_sorted else None
            second_favorite = fav_sorted[1] if len(fav_sorted) > 1 else None

            race_id = self._race_id(course_name, date_str, race_time, None)

            return RaceData(
                id=race_id,
                course=course_name,
                race_time=race_time,
                utc_datetime=utc_dt,
                local_time=local_dt.strftime("%H:%M"),
                timezone_name=tz_name,
                field_size=len(runners),
                country=country,
                discipline="thoroughbred",
                race_number=None,
                grade=None,
                distance=None,
                surface=None,
                favorite=favorite,
                second_favorite=second_favorite,
                all_runners=runners,
                race_url=race_url,
                data_sources={"course": "ATR", "runners": "ATR", "odds": "ATR"},
            )
        except Exception as e:
            logging.debug(f"ATR parse error: {e}")
            return None


class GBGreyhoundSource(DataSourceBase):
    """
    Sporting Life greyhound racecards (UK). Best-effort HTML parse:
    - Meeting index: https://www.sportinglife.com/greyhounds/racecards
    - Race links like: /greyhounds/racecards/{track-slug}/{YYYY-MM-DD}/{HHMM}
    """
    BASE = "https://www.sportinglife.com"
    INDEX = f"{BASE}/greyhounds/racecards"

    async def fetch_races(self, date_range: Tuple[datetime, datetime]) -> List[RaceData]:
        out: List[RaceData] = []
        html = await self.http_client.fetch(self.INDEX)
        if not html:
            return out
        soup = BeautifulSoup(html, "html.parser")
        meeting_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.match(r"^/greyhounds/racecards/[a-z0-9\-]+/\d{4}-\d{2}-\d{2}$", href):
                meeting_links.add(self.BASE + href)

        for m_url in sorted(meeting_links):
            m_html = await self.http_client.fetch(m_url)
            if not m_html:
                continue
            msoup = BeautifulSoup(m_html, "html.parser")
            race_links = set()
            for a in msoup.find_all("a", href=True):
                href = a["href"]
                if re.match(r"^/greyhounds/racecards/[a-z0-9\-]+/\d{4}-\d{2}-\d{2}/\d{3,4}$", href):
                    race_links.add(self.BASE + href)

            for r_url in sorted(race_links):
                try:
                    rd = await self._parse_race(r_url)
                    if rd:
                        out.append(rd)
                except Exception:
                    continue
        return out

    async def _parse_race(self, url: str) -> Optional[RaceData]:
        html = await self.http_client.fetch(url)
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")

        m = re.search(r"/greyhounds/racecards/([a-z0-9\-]+)/([\d]{4}-[\d]{2}-[\d]{2})/(\d{3,4})$", url)
        if not m:
            return None
        course_slug, date_str, hhmm = m.groups()
        course = course_slug.replace("-", " ").title()
        if len(hhmm) == 3:
            hhmm = "0" + hhmm
        race_time = f"{hhmm[:2]}:{hhmm[2:]}"

        runners: List[Dict[str, Any]] = []
        for tr in soup.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            if len(tds) < 2:
                continue
            name = tds[1].get_text(strip=True)
            if not name:
                continue
            odds_str = ""
            if len(tds) >= 3:
                odds_str = tds[-1].get_text(strip=True)
            runners.append({"name": name, "odds_str": odds_str})

        if not runners:
            return None

        fav_sorted = sorted(runners, key=lambda x: convert_odds_to_fractional(x.get("odds_str", "")))
        favorite = fav_sorted[0] if fav_sorted else None
        second_favorite = fav_sorted[1] if len(fav_sorted) > 1 else None

        tz_name = COUNTRY_TIMEZONES.get("GB", "Europe/London")
        local_tz = ZoneInfo(tz_name)
        local_dt = datetime.combine(datetime.strptime(date_str, "%Y-%m-%d").date(),
                                    datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=local_tz)
        utc_dt = local_dt.astimezone(ZoneInfo("UTC"))

        race_id = self._race_id(course, date_str, race_time, None)

        return RaceData(
            id=race_id,
            course=course,
            race_time=race_time,
            utc_datetime=utc_dt,
            local_time=local_dt.strftime("%H:%M"),
            timezone_name=tz_name,
            field_size=len(runners),
            country="GB",
            discipline="greyhound",
            race_number=None,
            grade=None,
            distance=None,
            surface=None,
            favorite=favorite,
            second_favorite=second_favorite,
            all_runners=runners,
            race_url=url,
            data_sources={"course": "SportingLife", "runners": "SportingLife", "odds": "SportingLife"},
        )


class HarnessRacingAustraliaSource(DataSourceBase):
    """
    Harness Racing Australia (fields) — best-effort parse of meeting/race pages.
    """
    BASE = "https://www.harness.org.au"

    async def fetch_races(self, date_range: Tuple[datetime, datetime]) -> List[RaceData]:
        out: List[RaceData] = []
        for dt in self._days(date_range):
            candidates = [
                f"{self.BASE}/racing/fields/?firstDate={dt.strftime('%d/%m/%Y')}&submit=DISPLAY",
                f"{self.BASE}/racing/fields/?meetingDate={dt.strftime('%Y-%m-%d')}",
            ]
            meeting_links = set()
            for url in candidates:
                html = await self.http_client.fetch(url)
                if not html:
                    continue
                soup = BeautifulSoup(html, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "/racing/fields/" in href and ("meeting" in href or "race" in href):
                        meeting_links.add(href if href.startswith("http") else self.BASE + href)

            for m_url in sorted(meeting_links):
                html = await self.http_client.fetch(m_url)
                if not html:
                    continue
                soup = BeautifulSoup(html, "html.parser")
                race_links = set()
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "/racing/fields/" in href and "race" in href:
                        race_links.add(href if href.startswith("http") else self.BASE + href)

                for r_url in sorted(race_links):
                    try:
                        rd = await self._parse_race(r_url, dt)
                        if rd:
                            out.append(rd)
                    except Exception:
                        continue
        return out

    def _days(self, date_range: Tuple[datetime, datetime]):
        cur = date_range[0].replace(hour=0, minute=0, second=0, microsecond=0)
        end = date_range[1].replace(hour=0, minute=0, second=0, microsecond=0)
        while cur <= end:
            yield cur
            cur += timedelta(days=1)

    async def _parse_race(self, url: str, dt: datetime) -> Optional[RaceData]:
        html = await self.http_client.fetch(url)
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")

        course = ""
        h = soup.find(["h1", "h2"])
        if h:
            course = h.get_text(strip=True)
        if not course:
            crumb = soup.find("div", class_=re.compile("breadcrumb|breadcrumbs", re.I))
            if crumb:
                course = crumb.get_text(" ", strip=True)
        if not course:
            course = "Harness Meeting"

        txt = soup.get_text(" ", strip=True)
        race_time = parse_local_hhmm(txt)
        if not race_time:
            return None

        runners: List[Dict[str, Any]] = []
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 2:
                name = tds[1].get_text(strip=True)
                if name:
                    runners.append({"name": name, "odds_str": ""})
        if not runners:
            for li in soup.find_all("li"):
                name = li.get_text(strip=True)
                if name and len(name.split()) > 1:
                    runners.append({"name": name, "odds_str": ""})
        if not runners:
            return None

        tz_name = self._track_tz(course, "AU")
        local_tz = ZoneInfo(tz_name)
        date_str = dt.strftime("%Y-%m-%d")
        local_dt = datetime.combine(dt.date(), datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=local_tz)
        utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
        race_id = self._race_id(course, date_str, race_time, None)

        return RaceData(
            id=race_id,
            course=course,
            race_time=race_time,
            utc_datetime=utc_dt,
            local_time=local_dt.strftime("%H:%M"),
            timezone_name=tz_name,
            field_size=len(runners),
            country="AU",
            discipline="harness",
            race_number=None,
            grade=None,
            distance=None,
            surface=None,
            favorite=None,
            second_favorite=None,
            all_runners=runners,
            race_url=url,
            data_sources={"course": "HRA", "runners": "HRA"},
        )


class StandardbredCanadaSource(DataSourceBase):
    """
    Standardbred Canada entries (harness). Best-effort scraping.
    """
    BASE = "https://standardbredcanada.ca"

    async def fetch_races(self, date_range: Tuple[datetime, datetime]) -> List[RaceData]:
        out: List[RaceData] = []
        for dt in self._days(date_range):
            index = f"{self.BASE}/racing/entries/date/{dt.strftime('%Y-%m-%d')}"
            html = await self.http_client.fetch(index)
            if not html:
                continue
            soup = BeautifulSoup(html, "html.parser")
            meeting_links = set()
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.match(r"^/racing/entries/[a-z0-9\-]+/\d{4}-\d{2}-\d{2}$", href):
                    meeting_links.add(self.BASE + href)

            for m_url in sorted(meeting_links):
                html = await self.http_client.fetch(m_url)
                if not html:
                    continue
                msoup = BeautifulSoup(html, "html.parser")
                sections = msoup.find_all(["section", "div"], id=re.compile(r"race-\d+", re.I))
                if not sections:
                    sections = [msoup]

                for sec in sections:
                    try:
                        rd = await self._parse_race_section(sec, m_url, dt)
                        if rd:
                            out.append(rd)
                    except Exception:
                        continue
        return out

    def _days(self, date_range: Tuple[datetime, datetime]):
        cur = date_range[0].replace(hour=0, minute=0, second=0, microsecond=0)
        end = date_range[1].replace(hour=0, minute=0, second=0, microsecond=0)
        while cur <= end:
            yield cur
            cur += timedelta(days=1)

    async def _parse_race_section(self, sec, page_url: str, dt: datetime) -> Optional[RaceData]:
        text = sec.get_text(" ", strip=True)
        course = ""
        root = sec
        try:
            while root.parent and root.parent.name not in ("html", "body"):
                root = root.parent
        except Exception:
            pass
        if hasattr(root, "find"):
            h = root.find(["h1", "h2"])
            if h:
                course = h.get_text(strip=True)
        if not course:
            title = None
            try:
                title = sec.find("title")
            except Exception:
                pass
            if title:
                course = title.get_text(strip=True)
        if not course:
            m = re.search(r"/racing/entries/([a-z0-9\-]+)/\d{4}-\d{2}-\d{2}", page_url)
            if m:
                course = m.group(1).replace("-", " ").title()
        if not course:
            course = "Standardbred Canada"

        race_time = parse_local_hhmm(text)
        if not race_time:
            return None

        runners: List[Dict[str, Any]] = []
        for tr in sec.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 2:
                name = tds[1].get_text(strip=True)
                if name:
                    runners.append({"name": name, "odds_str": ""})
        if not runners:
            for li in sec.find_all("li"):
                name = li.get_text(strip=True)
                if name and len(name.split()) > 1:
                    runners.append({"name": name, "odds_str": ""})
        if not runners:
            return None

        tz_name = self._track_tz(course, "CA")
        local_tz = ZoneInfo(tz_name)
        date_str = dt.strftime("%Y-%m-%d")
        local_dt = datetime.combine(dt.date(), datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=local_tz)
        utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
        race_id = self._race_id(course, date_str, race_time, None)

        return RaceData(
            id=race_id,
            course=course,
            race_time=race_time,
            utc_datetime=utc_dt,
            local_time=local_dt.strftime("%H:%M"),
            timezone_name=tz_name,
            field_size=len(runners),
            country="CA",
            discipline="harness",
            race_number=None,
            grade=None,
            distance=None,
            surface=None,
            favorite=None,
            second_favorite=None,
            all_runners=runners,
            race_url=page_url,
            data_sources={"course": "StandardbredCanada", "runners": "StandardbredCanada"},
        )


class RacingDataAggregator:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.cache = None if cfg.get("no_cache") else CacheManager(Path(cfg.get("cache_dir", DEFAULT_CACHE_DIR)))
        self.http = AsyncHttpClient(
            cfg.get("concurrency", 12),
            self.cache,
            verify_ssl=not cfg.get("insecure_ssl", False),
            http2=not cfg.get("no_http2", False),
        )
        self.scorer = ValueScorer(cfg.get("thresholds", {}))
        self.sources = [
            AtTheRacesSource(self.http),
            GBGreyhoundSource(self.http),
            HarnessRacingAustraliaSource(self.http),
            StandardbredCanadaSource(self.http),
        ]
        self.per_source_counts: Dict[str, int] = {}

    async def aclose(self):
        await self.http.aclose()

    async def fetch_all(self, start: datetime, end: datetime) -> List[RaceData]:
        tasks = [src.fetch_races((start, end)) for src in self.sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        races: List[RaceData] = []
        for src, res in zip(self.sources, results):
            if isinstance(res, Exception):
                logging.error(f"Source {src.name} failed: {res}")
                self.per_source_counts[src.name] = 0
                continue
            self.per_source_counts[src.name] = len(res)
            races.extend(res)

        merged = self._dedupe_merge(races)
        await self._enrich_rs_links(merged)

        for r in merged:
            r.value_score = self.scorer.calculate_score(r)
        merged.sort(key=lambda r: r.value_score, reverse=True)
        return merged

    def _key(self, race: RaceData) -> str:
        course = self._norm_course(race.course)
        date = race.utc_datetime.strftime("%Y-%m-%d")
        t_norm = self._round_time(race.utc_datetime.strftime("%H:%M"))
        num = str(race.race_number or "")
        return "|".join([course, date, t_norm, num])

    def _norm_course(self, name: str) -> str:
        if not name:
            return ""
        normalized = re.sub(r"\s*\([^)]*\)", "", name.lower().strip())
        normalized = normalized.replace("-", " ").replace("_", " ")
        return re.sub(r"\s+", " ", normalized)

    def _round_time(self, time_str: str, tol_min: int = 5) -> str:
        try:
            t = datetime.strptime(time_str, "%H:%M")
            rounded = t.replace(minute=(t.minute // tol_min) * tol_min, second=0)
            return rounded.strftime("%H:%M")
        except Exception:
            return time_str

    def _merge(self, a: RaceData, b: RaceData) -> RaceData:
        primary, secondary = (a, b) if len(a.data_sources) >= len(b.data_sources) else (b, a)
        merged_sources = {**secondary.data_sources, **primary.data_sources}
        merged = RaceData(
            id=primary.id,
            course=primary.course or secondary.course,
            race_time=primary.race_time or secondary.race_time,
            utc_datetime=primary.utc_datetime,
            local_time=primary.local_time or secondary.local_time,
            timezone_name=primary.timezone_name or secondary.timezone_name,
            field_size=max(primary.field_size, secondary.field_size),
            country=primary.country or secondary.country,
            discipline=primary.discipline or secondary.discipline,
            race_number=primary.race_number or secondary.race_number,
            grade=primary.grade or secondary.grade,
            distance=primary.distance or secondary.distance,
            surface=primary.surface or secondary.surface,
            favorite=primary.favorite or secondary.favorite,
            second_favorite=primary.second_favorite or secondary.second_favorite,
            all_runners=primary.all_runners if len(primary.all_runners) >= len(secondary.all_runners) else secondary.all_runners,
            race_url=primary.race_url or secondary.race_url,
            form_guide_url=primary.form_guide_url or secondary.form_guide_url,
            data_sources=merged_sources,
        )
        return merged

    def _dedupe_merge(self, races: List[RaceData]) -> List[RaceData]:
        m: Dict[str, RaceData] = {}
        for r in races:
            k = self._key(r)
            if k not in m:
                m[k] = r
            else:
                m[k] = self._merge(m[k], r)
        return list(m.values())

    async def _enrich_rs_links(self, races: List[RaceData]) -> None:
        url = "https://www.racingandsports.com.au/todays-racing-json-v2"
        try:
            text = await self.http.fetch(url)
            if not text:
                return
            payload = json.loads(text)
            lookup: Dict[Tuple[str, str], str] = {}
            for disc in payload or []:
                for country in disc.get("Countries", []):
                    for meet in country.get("Meetings", []):
                        course = meet.get("Course")
                        link = meet.get("PDFUrl") or meet.get("PreMeetingUrl")
                        if not (course and link):
                            continue
                        m = re.search(r"/(\d{4}-\d{2}-\d{2})", link or "")
                        if not m:
                            continue
                        date = m.group(1)
                        key = (self._norm_course(course), date)
                        lookup[key] = link
            for r in races:
                date = r.utc_datetime.astimezone(ZoneInfo(r.timezone_name)).strftime("%Y-%m-%d")
                key = (self._norm_course(r.course), date)
                if key in lookup and not r.form_guide_url:
                    r.form_guide_url = lookup[key]
                    r.data_sources["form"] = "R&S"
        except Exception as e:
            logging.debug(f"R&S enrichment failed: {e}")


class OutputManager:
    def __init__(self, out_dir: Path, cfg: Dict[str, Any], thresholds: Dict[str, Any]):
        self.out_dir = out_dir
        self.out_dir.mkdir(exist_ok=True, parents=True)
        self.cfg = cfg
        self.th = thresholds

    async def write_all(self, races: List[RaceData], stats: Dict[str, Any], formats: List[str]) -> None:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        tasks = []
        if "html" in formats:
            tasks.append(self._write_html(races, stats, f"racing_report_{stamp}.html"))
        if "json" in formats:
            tasks.append(self._write_json(races, stats, f"racing_report_{stamp}.json"))
        if "csv" in formats:
            tasks.append(self._write_csv(races, f"racing_report_{stamp}.csv"))
        await asyncio.gather(*tasks)

    async def _write_json(self, races: List[RaceData], stats: Dict[str, Any], fname: str):
        doc = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "config": self.cfg,
            "statistics": stats,
            "races": [self._race_to_dict(r) for r in races],
        }
        async with aiofiles.open(self.out_dir / fname, "w", encoding="utf-8") as f:
            await f.write(json.dumps(doc, indent=2, default=str, ensure_ascii=False))
        logging.info(f"Wrote JSON {fname}")

    async def _write_csv(self, races: List[RaceData], fname: str):
        path = self.out_dir / fname

        def _write():
            fields = [
                "id", "course", "country", "discipline",
                "race_time", "local_time", "timezone_name",
                "field_size", "value_score",
                "favorite_name", "favorite_odds",
                "second_favorite_name", "second_favorite_odds",
                "race_url", "form_guide_url", "data_sources",
            ]
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                for r in races:
                    fav = r.favorite or {}
                    sec = r.second_favorite or {}
                    w.writerow({
                        "id": r.id,
                        "course": r.course,
                        "country": r.country,
                        "discipline": r.discipline,
                        "race_time": r.race_time,
                        "local_time": r.local_time,
                        "timezone_name": r.timezone_name,
                        "field_size": r.field_size,
                        "value_score": f"{r.value_score:.1f}",
                        "favorite_name": fav.get("name", ""),
                        "favorite_odds": fav.get("odds_str", ""),
                        "second_favorite_name": sec.get("name", ""),
                        "second_favorite_odds": sec.get("odds_str", ""),
                        "race_url": r.race_url,
                        "form_guide_url": r.form_guide_url or "",
                        "data_sources": json.dumps(r.data_sources, ensure_ascii=False),
                    })
        await asyncio.to_thread(_write)
        logging.info(f"Wrote CSV {fname}")

    def _match_flag(self, r: RaceData) -> bool:
        max_field = int(self.th.get("max_field_size", 7))
        min_fav = float(self.th.get("min_fav_fractional", 1.0))
        min_sec = float(self.th.get("min_second_fav_fractional", 3.0))
        min_ratio = float(self.th.get("min_odds_ratio", 0.0))
        if r.field_size >= max_field:
            return False
        fav_f = convert_odds_to_fractional((r.favorite or {}).get("odds_str", ""))
        sec_f = convert_odds_to_fractional((r.second_favorite or {}).get("odds_str", ""))
        if fav_f == 999.0 or sec_f == 999.0:
            return False
        if fav_f < min_fav:
            return False
        if sec_f < min_sec:
            return False
        if min_ratio > 0 and (sec_f / max(1e-9, fav_f)) < min_ratio:
            return False
        return True

    def _generate_html(self, races: List[RaceData], stats: Dict[str, Any]) -> str:
        icons = {"thoroughbred": "Ⓣ", "greyhound": "Ⓖ", "harness": "Ⓗ"}
        css = """
        <style>
        :root{color-scheme:light dark}
        body{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;background:#f7f7f9;color:#222;margin:20px}
        @media (max-width:640px){body{margin:8px}}
        .container{max-width:1080px;margin:auto;background:#fff;padding:20px;border-radius:8px;box-shadow:0 2px 12px rgba(0,0,0,.06)}
        h1{margin:0 0 16px}
        .meta{color:#666;font-size:.9rem;margin-bottom:16px}
        .race{border:1px solid #e6e6ef;border-left:4px solid #5b8def;border-radius:6px;padding:12px;margin:10px 0;background:#fff}
        .race.match{border-left-color:#2fb344;background:#f8fff8}
        .head{display:flex;flex-wrap:wrap;align-items:center;gap:8px;font-weight:600}
        .pill{display:inline-block;padding:2px 8px;border-radius:999px;background:#eef1f6;color:#334;font-size:.85rem}
        .links a{display:inline-block;margin-right:8px;margin-top:8px;text-decoration:none;color:#fff;background:#007bff;padding:6px 10px;border-radius:4px}
        .links a.alt{background:#6c757d}
        .kv{font-size:.95rem;margin-top:6px}
        .kv b{color:#333}
        .ex{margin-top:6px;color:#333}
        </style>
        """
        header = f"<h1>Global Racing Report</h1><div class='meta'>Generated {datetime.now().isoformat(timespec='seconds')} • Races: {len(races)} • Cache hits: {stats.get('cache_hits',0)} • Misses: {stats.get('cache_misses',0)} • Runtime: {stats.get('duration_seconds',0):.1f}s</div>"
        items: List[str] = []
        for r in races:
            fav = r.favorite or {}
            sec = r.second_favorite or {}
            icon = icons.get(r.discipline, "Ⓣ")
            match_class = "match" if self._match_flag(r) else ""
            links = [f"<a href='{r.race_url}' target='_blank' rel='noopener'>Racecard</a>"]
            if r.form_guide_url:
                links.append(f"<a class='alt' href='{r.form_guide_url}' target='_blank' rel='noopener'>Form</a>")
            block = f"""
            <div class='race {match_class}'>
              <div class='head'>{icon} {r.course} ({r.country})
                <span class='pill'>{r.local_time} {r.timezone_name}</span>
                <span class='pill'>Field {r.field_size}</span>
                <span class='pill'>Score {r.value_score:.1f}</span>
              </div>
              <div class='kv'>
                <b>Fav:</b> {fav.get('name','')} ({fav.get('odds_str','')}) &nbsp;&nbsp;
                <b>2nd:</b> {sec.get('name','')} ({sec.get('odds_str','')})
              </div>
              <div class='links'>{''.join(links)}</div>
            </div>
            """
            items.append(block)
        return f"<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>Global Racing Report</title>{css}</head><body><div class='container'>{header}{''.join(items)}</div></body></html>"

    async def _write_html(self, races: List[RaceData], stats: Dict[str, Any], fname: str):
        html = self._generate_html(races, stats)
        async with aiofiles.open(self.out_dir / fname, "w", encoding="utf-8") as f:
            await f.write(html)
        logging.info(f"Wrote HTML {fname}")

    def _race_to_dict(self, r: RaceData) -> Dict[str, Any]:
        d = asdict(r)
        d["utc_datetime"] = r.utc_datetime.isoformat()
        return d


# ---------------- CLI / Main ----------------

async def _amain(args):
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s: %(message)s")

    if args.insecure_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if args.disable_browser_fetch:
        os.environ["DISABLE_BROWSER_FETCH"] = "1"

    start = (datetime.now() - timedelta(days=max(0, args.days_back - 1))).replace(hour=0, minute=0, second=0, microsecond=0)
    end = datetime.now()

    thresholds = {
        "max_field_size": args.max_field_size,
        "min_fav_fractional": args.min_fav_fractional,
        "min_second_fav_fractional": args.min_second_fav_fractional,
        "min_odds_ratio": args.min_odds_ratio,
    }
    cfg = {
        "cache_dir": args.cache_dir,
        "concurrency": args.concurrency,
        "thresholds": thresholds,
        "formats": args.formats,
        "no_cache": args.no_cache,
        "insecure_ssl": args.insecure_ssl,
        "no_http2": args.no_http2,
    }

    t0 = time.perf_counter()
    agg = RacingDataAggregator(cfg)
    try:
        races = await agg.fetch_all(start, end)
    finally:
        await agg.aclose()
    duration = time.perf_counter() - t0

    stats = {
        "cache_hits": agg.cache.hit_count if agg.cache else 0,
        "cache_misses": agg.cache.miss_count if agg.cache else 0,
        "http_success": agg.http.success_count,
        "http_retries": agg.http.retry_count,
        "http_blocked": agg.http.blocked_count,
        "browser_attempts": agg.http.browser_attempts,
        "browser_success": agg.http.browser_success,
        "races_total": len(races),
        "duration_seconds": duration,
        "per_source_counts": getattr(agg, "per_source_counts", {}),
    }

    out = OutputManager(Path(args.output_dir), cfg, thresholds)
    await out.write_all(races, stats, args.formats)


def parse_args():
    p = argparse.ArgumentParser(description="Melt & Repour Global Racing Scanner V2.2")
    p.add_argument("--days-back", type=int, default=2, help="Days back to scan (inclusive)")
    p.add_argument("--concurrency", type=int, default=12, help="Max concurrent HTTP requests")
    p.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR), help="Cache directory")
    p.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory")
    p.add_argument("--formats", nargs="+", default=["html","json","csv"], help="Output formats")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    # thresholds
    p.add_argument("--max-field-size", type=int, default=7)
    p.add_argument("--min-fav-fractional", type=float, default=1.0)
    p.add_argument("--min-second-fav-fractional", type=float, default=3.0)
    p.add_argument("--min-odds-ratio", type=float, default=0.0)
    # networking / cache toggles
    p.add_argument("--no-cache", action="store_true", help="Disable on-disk cache")
    p.add_argument("--insecure-ssl", action="store_true", help="Disable SSL verification (not recommended)")
    p.add_argument("--no-http2", action="store_true", help="Disable HTTP/2")
    p.add_argument("--disable-browser-fetch", action="store_true", help="Disable browser fallback")
    return p.parse_args()


def main():
    args = parse_args()
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()