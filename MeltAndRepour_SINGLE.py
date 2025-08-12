#!/usr/bin/env python3
"""
Melt & Repour Global Racing Scanner V3.1 - SINGLE FILE

- PATCHED: Fixed 12-hour time parsing with AM/PM awareness for AU/CA harness sources
- PATCHED: Added meta-refresh redirect handling for HRA redirect carousel
- PATCHED: Hardened HRA fetches with fragment stripping and headless fallback
- PATCHED: Standardbred Canada index fallback for handling 404 dates
- PATCHED: Corrected dedup key to avoid over-merging races
- PATCHED: Enhanced SkySportsSource robustness with fallback counting and date-range index coverage
- PATCHED: Updated timezone mappings for Australian tracks
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
import shutil
import subprocess
import certifi
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from zoneinfo import ZoneInfo
import time
from urllib.parse import urlparse, urljoin
import sys

import httpx
import aiofiles
from bs4 import BeautifulSoup
from bs4.element import Tag
from curl_cffi.requests import Session as CurlCffiSession

# Silence urllib3 warnings when user disables SSL verification
import urllib3

SCHEMA_VERSION = "3.1"
DEFAULT_CACHE_DIR = Path(".cache")
DEFAULT_OUTPUT_DIR = Path("output")
REQUEST_TIMEOUT = 30.0

# Track timezone mapping (seed data; grow over time)
TRACK_TIMEZONES = {
    # UK & Ireland
    "ayr": "Europe/London", "kempton-park": "Europe/London", "windsor": "Europe/London",
    "ascot": "Europe/London", "cheltenham": "Europe/London", "newmarket": "Europe/London",
    "leopardstown": "Europe/Dublin", "curragh": "Europe/Dublin", "ballinrobe": "Europe/Dublin",
    # USA (examples)
    "finger-lakes": "America/New_York", "fort-erie": "America/Toronto", "presque-isle-downs": "America/New_York",
    "ellis-park": "America/Chicago", "thistledown": "America/New_York", "mountaineer-park": "America/New_York",
    "mountaineer": "America/New_York",
    "churchill": "America/New_York", "belmont": "America/New_York", "saratoga": "America/New_York",
    "santa-anita": "America/Los_Angeles", "del-mar": "America/Los_Angeles",
    # France
    "la-teste-de-buch": "Europe/Paris", "clairefontaine": "Europe/Paris", "cagnes-sur-mer-midi": "Europe/Paris",
    "divonne-les-bains": "Europe/Paris", "longchamp": "Europe/Paris", "saint-malo": "Europe/Paris",
    # Australia - PATCHED: Enhanced AU harness TZs
    "flemington": "Australia/Melbourne", "randwick": "Australia/Sydney", "eagle-farm": "Australia/Brisbane",
    "albion-park": "Australia/Brisbane", "redcliffe": "Australia/Brisbane",
    "menangle": "Australia/Sydney", "gloucester-park": "Australia/Perth",
    # South Africa
    "fairview": "Africa/Johannesburg",
    # Other
    "gavea": "America/Sao_Paulo", "sha-tin": "Asia/Hong_Kong", "tokyo": "Asia/Tokyo",
}

COUNTRY_TIMEZONES = {
    "GB": "Europe/London", "IE": "Europe/Dublin", "US": "America/New_York", "FR": "Europe/Paris",
    "AU": "Australia/Sydney", "NZ": "Pacific/Auckland", "HK": "Asia/Hong_Kong",
    "JP": "Asia/Tokyo", "ZA": "Africa/Johannesburg", "CA": "America/Toronto", "BR": "America/Sao_Paulo",
}

def resolve_chrome_binary() -> Optional[str]:
    import shutil
    for p in [
        os.getenv("GOOGLE_CHROME_BIN"), os.getenv("CHROME_BIN"), "/usr/bin/google-chrome-stable",
        "/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser",
        r"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
        r"C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
    ]:
        if p and os.path.exists(p):
            return p
    return shutil.which("google-chrome") or shutil.which("google-chrome-stable") or shutil.which("chromium") or None

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

# PATCHED: Fix 12-hour times with AM/PM-aware parsing

def parse_local_hhmm(time_text: str) -> Optional[str]:
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

# PATCHED: Helper for meta-refresh redirect extraction (more tolerant)

def _extract_meta_refresh_target(html: str) -> Optional[str]:
    m = re.search(r'http-equiv=["\']?refresh["\']?[^>]*content=["\']?\s*\d+\s*;\s*url=([^"\'>\s]+)', html, re.I)
    return m.group(1).strip() if m else None

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
    discipline: str
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
                k.lower(): v
                for k, v in dict(headers).items()
                if k and k.lower() in {"etag", "last-modified", "content-type", "cache-control"}
            },
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
        self.fallback_success = 0
        self.ua_pool = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        ]
        self._ua_idx = 0
        self._client: Optional[httpx.AsyncClient] = None
        self.verify_ssl = verify_ssl
        self.http2 = http2
        self._host_last: Dict[str, float] = {}
        self._throttle_lock = asyncio.Lock()
        self.min_interval_per_host = 0.25

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
                },
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
            self._host_last[host] = time.perf_counter()

    async def fetch(self, url: str, use_browser: bool = False) -> Optional[str]:
        async with self.semaphore:
            if self.cache_manager and (cached := await self.cache_manager.get(url)):
                return cached[0]

            if content := await self._fetch_http(url):
                self.success_count += 1
                return content

            if content := await self._fetch_fallback(url):
                self.success_count += 1
                self.fallback_success += 1
                if self.cache_manager:
                    await self.cache_manager.set(url, content, headers={"cache-control": "max-age=600"})
                return content

            if use_browser and os.getenv("DISABLE_BROWSER_FETCH") != "1":
                self.browser_attempts += 1
                if content := await self._fetch_browser(url):
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
        if self.cache_manager and (cached := await self.cache_manager.get(url)) and cached[1]:
            h = cached[1]
            if "etag" in h:
                headers["If-None-Match"] = h["etag"]
            if "last-modified" in h:
                headers["If-Modified-Since"] = h["last-modified"]
        client = await self._client_get()
        for attempt in range(4):
            try:
                r = await client.get(url, headers=headers)
                if r.status_code == 304 and self.cache_manager and (cached := await self.cache_manager.get(url)):
                    return cached[0]
                r.raise_for_status()
                text = r.text

                # PATCHED: Follow meta-refresh redirects (fixes HRA redirect carousel)
                target = _extract_meta_refresh_target(text)
                if target:
                    try:
                        next_url = urljoin(str(r.request.url), target)
                        for _ in range(2):
                            await asyncio.sleep(0.05)
                            await self._throttle(next_url)
                            rr = await client.get(next_url, headers={"User-Agent": self._ua()})
                            rr.raise_for_status()
                            tt = rr.text
                            nxt = _extract_meta_refresh_target(tt)
                            if not nxt:
                                if self.cache_manager and rr.status_code == 200:
                                    await self.cache_manager.set(next_url, tt, rr.headers)
                                return tt
                            next_url = urljoin(str(rr.request.url), nxt)
                    except Exception:
                        pass

                if self.cache_manager and r.status_code == 200:
                    await self.cache_manager.set(url, text, r.headers)
                return text
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (429, 503, 502, 500, 520, 521, 522):
                    self.retry_count += 1
                    await asyncio.sleep((2 ** attempt) + random.random() * 0.3)
                    continue
                logging.debug(f"HTTP status {e.response.status_code} for {url}")
                break
            except (httpx.TimeoutException, httpx.RequestError):
                self.retry_count += 1
                await asyncio.sleep(0.5 + random.random() * 0.25)
                continue
        return None

    def _try_curl_cffi_sync(self, url: str) -> Optional[str]:
        try:
            session = CurlCffiSession(impersonate="chrome120", timeout=20)
            headers = {
                'User-Agent': self._ua(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            }
            response = session.get(url, headers=headers, verify=certifi.where())
            response.raise_for_status()
            return response.text
        except Exception as e:
            logging.debug(f"curl_cffi_sync failed for {url}: {e}")
            return None

    def _try_subprocess_curl_sync(self, url: str) -> Optional[str]:
        if not shutil.which("curl"):
            return None
        try:
            curl_cmd = [
                'curl', '-s', '-L', '--compressed', '-m', '30',
                '-A', self._ua(), '-H', 'Accept: text/html', '--ssl-no-revoke', url
            ]
            result = subprocess.run(
                curl_cmd, capture_output=True, text=True, timeout=45, check=False, encoding='utf-8', errors='ignore'
            )
            return result.stdout if result.returncode == 0 and result.stdout else None
        except Exception as e:
            logging.debug(f"subprocess_curl_sync failed for {url}: {e}")
            return None

    async def _fetch_fallback(self, url: str) -> Optional[str]:
        loop = asyncio.get_running_loop()
        if content := await loop.run_in_executor(None, self._try_curl_cffi_sync, url):
            logging.debug(f"Fallback success (curl_cffi) for {url}")
            return content
        if content := await loop.run_in_executor(None, self._try_subprocess_curl_sync, url):
            logging.debug(f"Fallback success (subprocess) for {url}")
            return content
        return None

    async def _fetch_browser(self, url: str) -> Optional[str]:
        await self._throttle(url)
        if not (chrome_bin := resolve_chrome_binary()):
            return None
        try:
            import undetected_chromedriver as uc
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            options = uc.ChromeOptions()
            options.binary_location = chrome_bin
            for arg in ["--headless=new", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]:
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
            score += min(100.0, max(0.0, (fav_f - 0.5) / 3.5 * 100)) * self.weights["odds_value"]
        if fav_f != 999.0 and sec_f != 999.0 and sec_f > fav_f:
            score += min(100.0, max(0.0, (sec_f - fav_f)) / 5 * 100) * self.weights["odds_spread"]
        score += min(100.0, len(race.data_sources) * 25.0) * self.weights["data_quality"]
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
        return TRACK_TIMEZONES.get(key) or COUNTRY_TIMEZONES.get(country.upper(), "UTC")

    def _race_id(self, course: str, date: str, time_s: str) -> str:
        parts = [self._normalize_course_name(course), date, re.sub(r"[^\d]", "", time_s or "")]
        return hashlib.sha256("|".join(parts).encode()).hexdigest()[:12]

    async def fetch_races(self, date_range: Tuple[datetime, datetime]) -> List[RaceData]:
        raise NotImplementedError

    def _days(self, date_range: Tuple[datetime, datetime]):
        cur = date_range[0].date()
        end = date_range[1].date()
        while cur <= end:
            yield datetime.combine(cur, datetime.min.time())
            cur += timedelta(days=1)

class AtTheRacesSource(DataSourceBase):
    REGIONS = ["uk", "ireland", "usa", "france", "saf", "aus"]

    async def fetch_races(self, date_range: Tuple[datetime, datetime]) -> List[RaceData]:
        out: List[RaceData] = []
        for dt in self._days(date_range):
            tasks = [
                self._fetch_region(
                    f"https://www.attheraces.com/ajax/marketmovers/tabs/{region}/{dt.strftime('%Y%m%d')}",
                    region,
                    dt,
                )
                for region in self.REGIONS
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            any_added = False
            for res in results:
                if isinstance(res, list):
                    if res:
                        any_added = True
                    out.extend(res)
                elif isinstance(res, Exception):
                    logging.debug(f"ATR region fetch error: {res}")
            # Fallback if marketmovers produced nothing for this date
            if not any_added:
                try:
                    fallback = await self._fallback_racecards_date(dt)
                    if fallback:
                        out.extend(fallback)
                except Exception as e:
                    logging.debug(f"ATR fallback error for {dt.date()}: {e}")
        logging.info(f"AtTheRaces total races for window: {len(out)}")
        return out

    async def _fetch_region(self, url: str, region: str, dt: datetime) -> List[RaceData]:
        html = await self.http_client.fetch(url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        races: List[RaceData] = []
        for caption in soup.find_all("caption", string=re.compile(r"^\d{2}:\d{2}")):
            if rd := self._parse_from_caption(caption, region, dt):
                races.append(rd)
        return races

    def _parse_from_caption(self, caption: Tag, region: str, dt: datetime) -> Optional[RaceData]:
        try:
            if not (m := re.match(r"(\d{2}:\d{2})", caption.get_text(strip=True))):
                return None
            race_time = m.group(1)
            panel = caption.find_parent("div", class_=re.compile(r"\bpanel\b")) or caption.find_parent("div")
            course_header = panel.find("h2") if panel else caption.find_previous("h2")
            if not course_header:
                return None
            course_name = course_header.get_text(strip=True)
            table = caption.find_next_sibling("table") or caption.find_parent().find("table")
            if not table:
                return None
            tbody = table.find("tbody") or table
            rows = tbody.find_all("tr") if tbody else []
            runners = [
                {"name": cells[0].get_text(strip=True), "odds_str": cells[1].get_text(strip=True)}
                for row in rows
                if (cells := row.find_all(["td", "th"])) and len(cells) >= 2 and cells[0].get_text(strip=True)
            ]
            if not runners:
                return None
            country_map = {"uk": "GB", "ireland": "IE", "usa": "US", "france": "FR", "saf": "ZA", "aus": "AU"}
            country = country_map.get(region, "GB")
            tz_name = self._track_tz(course_name, country)
            local_tz = ZoneInfo(tz_name)
            local_dt = datetime.combine(dt.date(), datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=local_tz)
            utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
            date_str = dt.strftime("%Y-%m-%d")
            fav_sorted = sorted(runners, key=lambda x: convert_odds_to_fractional(x.get("odds_str", "")))
            race_url = (
                f"https://www.attheraces.com/racecard/{re.sub(r'\s+', '-', course_name.strip().lower())}/"
                f"{date_str}/{race_time.replace(':', '')}"
            )
            return RaceData(
                id=self._race_id(course_name, date_str, race_time),
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
                favorite=fav_sorted[0] if fav_sorted else None,
                second_favorite=fav_sorted[1] if len(fav_sorted) > 1 else None,
                all_runners=runners,
                race_url=race_url,
                data_sources={"course": "ATR", "runners": "ATR", "odds": "ATR"},
            )
        except Exception as e:
            logging.warning(f"ATR parse error: {e}")
            return None

    async def _fallback_racecards_date(self, dt: datetime) -> List[RaceData]:
        # Fetch generic date page and parse individual race links
        date_str = dt.strftime("%Y-%m-%d")
        index_url = f"https://www.attheraces.com/racecards/{date_str}"
        html = await self.http_client.fetch(index_url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        race_links: List[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.match(r"^/racecard/[a-z0-9\-]+/\d{4}-\d{2}-\d{2}/\d{3,4}$", href):
                race_links.append(urljoin("https://www.attheraces.com", href))
        # Deduplicate and limit to be polite
        uniq_links = sorted(set(race_links))[:80]
        results = await asyncio.gather(*[self._fallback_parse_race(url) for url in uniq_links])
        return [r for r in results if r]

    async def _fallback_parse_race(self, url: str) -> Optional[RaceData]:
        html = await self.http_client.fetch(url)
        if not html:
            return None
        try:
            m = re.search(r"/racecard/([a-z0-9\-]+)/([\d\-]+)/([0-9]{3,4})$", url)
            if not m:
                return None
            course_slug, date_str, hhmm = m.groups()
            course_name = course_slug.replace("-", " ").title()
            race_time = ("0" * (4 - len(hhmm)) + hhmm)
            race_time = f"{race_time[:2]}:{race_time[2:]}"
            soup = BeautifulSoup(html, "html.parser")
            # Count runner rows from table(s)
            runners: List[Dict[str, Any]] = []
            for table in soup.find_all("table"):
                for tr in table.find_all("tr"):
                    cells = tr.find_all(["td", "th"])
                    if len(cells) >= 2 and cells[0].get_text(strip=True) and cells[1].get_text(strip=True):
                        name = cells[1].get_text(strip=True)
                        if name:
                            runners.append({"name": name, "odds_str": ""})
            # Cap plausible field size; discard if out of range
            if len(runners) < 3 or len(runners) > 20:
                return None
            tz_name = self._track_tz(course_name, "GB")
            local_tz = ZoneInfo(tz_name)
            local_dt = datetime.combine(datetime.fromisoformat(date_str).date(), datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=local_tz)
            return RaceData(
                id=self._race_id(course_name, date_str, race_time),
                course=course_name,
                race_time=race_time,
                utc_datetime=local_dt.astimezone(ZoneInfo("UTC")),
                local_time=local_dt.strftime("%H:%M"),
                timezone_name=tz_name,
                field_size=len(runners),
                country="GB",
                discipline="thoroughbred",
                race_number=None,
                grade=None,
                distance=None,
                surface=None,
                favorite=None,
                second_favorite=None,
                all_runners=runners,
                race_url=url,
                data_sources={"course": "ATR", "runners": "ATR"},
            )
        except Exception:
            return None

# New: Sporting Life Horse API source (global TB)
class SportingLifeHorseApiSource(DataSourceBase):
    BASE = "https://www.sportinglife.com/api/horse-racing/race"

    COURSE_TO_COUNTRY = {
        "gavea": "BR",
        "mountaineer": "US",
        "mountaineer park": "US",
        "fairview": "ZA",
        "saint-malo": "FR",
    }

    async def fetch_races(self, date_range: Tuple[datetime, datetime]) -> List[RaceData]:
        out: List[RaceData] = []
        for dt in self._days(date_range):
            # SL API often supports sorting/paging; fetch a reasonable batch
            url = f"{self.BASE}?limit=200&sort_direction=ASC&sort_field=RACE_TIME"
            text = await self.http_client.fetch(url)
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                logging.debug("SportingLifeHorseApi JSON decode failed")
                continue
            for item in payload if isinstance(payload, list) else []:
                try:
                    rs = item.get("race_summary", {})
                    course = (rs.get("course_name") or "").strip()
                    date_str = rs.get("date") or ""
                    time_str = rs.get("time") or ""
                    ride_count = int(rs.get("ride_count") or 0)
                    if not course or not date_str or not time_str or ride_count <= 0:
                        continue
                    # Filter to current day window
                    try:
                        d_date = datetime.fromisoformat(date_str).date()
                        if not (date_range[0].date() <= d_date <= date_range[1].date()):
                            continue
                    except Exception:
                        pass
                    slug = re.sub(r"\s+", "-", course.lower())
                    hhmm = time_str.replace(":", "")
                    race_url = f"https://www.sportinglife.com/racing/racecards/{slug}/{date_str}/{hhmm}"
                    # Build runners from rides
                    runners: List[Dict[str, Any]] = []
                    for ride in item.get("rides", []) or []:
                        horse = (ride.get("horse") or {}).get("name") or ""
                        odds = ((ride.get("betting") or {}).get("current_odds") or "").strip()
                        if horse:
                            runners.append({"name": horse, "odds_str": odds})
                    fav_sorted = sorted(runners, key=lambda x: convert_odds_to_fractional(x.get("odds_str", ""))) if runners else []
                    # Country and timezone
                    course_key = course.lower()
                    country = self.COURSE_TO_COUNTRY.get(course_key) or "GB"
                    tz_name = self._track_tz(course, country)
                    local_tz = ZoneInfo(tz_name)
                    local_dt = datetime.combine(datetime.fromisoformat(date_str).date(), datetime.strptime(time_str, "%H:%M").time()).replace(tzinfo=local_tz)
                    out.append(
                        RaceData(
                            id=self._race_id(course, date_str, time_str),
                            course=course,
                            race_time=time_str,
                            utc_datetime=local_dt.astimezone(ZoneInfo("UTC")),
                            local_time=local_dt.strftime("%H:%M"),
                            timezone_name=tz_name,
                            field_size=ride_count,
                            country=country,
                            discipline="thoroughbred",
                            race_number=None,
                            grade=None,
                            distance=rs.get("distance"),
                            surface=(rs.get("course_surface") or {}).get("surface"),
                            favorite=fav_sorted[0] if fav_sorted else None,
                            second_favorite=fav_sorted[1] if len(fav_sorted) > 1 else None,
                            all_runners=runners,
                            race_url=race_url,
                            data_sources={"course": "SL API", "runners": "SL API", "odds": "SL API"},
                        )
                    )
                except Exception:
                    continue
        logging.info(f"SportingLife Horse API total races for window: {len(out)}")
        return out

class SkySportsSource(DataSourceBase):
    BASE = "https://www.skysports.com/racing/racecards"

    async def fetch_races(self, date_range: Tuple[datetime, datetime]) -> List[RaceData]:
        races: List[RaceData] = []
        for dt in self._days(date_range):
            # Try multiple index variants for the given date
            index_candidates = [
                self.BASE if dt.date() == datetime.now().date() else None,
                f"{self.BASE}/{dt.strftime('%d-%m-%Y')}",
                f"{self.BASE}?date={dt.strftime('%Y-%m-%d')}",
            ]
            index_candidates = [u for u in index_candidates if u]
            html = None
            for idx in index_candidates:
                html = await self.http_client.fetch(idx)
                if html:
                    break
            if not html:
                continue
            soup = BeautifulSoup(html, 'html.parser')
            for container in soup.find_all('div', class_='sdc-site-racing-meetings__event'):
                if rd := await self._parse_race_entry(container):
                    races.append(rd)
        logging.info(f"SkySports total races for window: {len(races)}")
        return races

    async def _parse_race_entry(self, container: Tag) -> Optional[RaceData]:
        try:
            racecard_tag = container.find('a', class_='sdc-site-racing-meetings__event-link')
            if not (racecard_tag and racecard_tag.get('href')):
                return None

            race_url = urljoin(self.BASE, racecard_tag['href'])
            details_span = container.find('span', class_='sdc-site-racing-meetings__event-details')
            details_text = details_span.get_text(strip=True) if details_span else ""

            runner_match = re.search(r'(\d+)\s+runners?', details_text, re.I)
            field_size = int(runner_match.group(1)) if runner_match else 0

            # PATCHED: Enhanced SkySportsSource robustness - fallback to counting declared runners
            if field_size == 0:
                rc_html = await self.http_client.fetch(race_url)
                if rc_html:
                    rc = BeautifulSoup(rc_html, "html.parser")
                    declared = rc.select("table tbody tr") or rc.select("tbody tr")
                    cnt = len([r for r in declared if r.find('td')])
                    if cnt > 0:
                        field_size = cnt
            if field_size == 0:
                return None

            race_time = parse_local_hhmm(details_text)
            if not race_time:
                return None

            # Parse URL for date and course
            path_parts = urlparse(race_url).path.strip('/').split('/')
            course_name, date_str = "Unknown", datetime.now().strftime("%Y-%m-%d")
            try:
                racecards_index = path_parts.index('racecards')
                course_name = path_parts[racecards_index + 1].replace('-', ' ').title()
                date_str = path_parts[racecards_index + 2]
                dt_obj = datetime.strptime(date_str, "%d-%m-%Y")  # Sky uses DD-MM-YYYY
            except (ValueError, IndexError):
                logging.warning(f"Could not parse date/course from Sky URL: {race_url}")
                return None

            country_code_match = re.search(r'\(([A-Z]{2,3})\)', details_text)
            country = country_code_match.group(1) if country_code_match else "GB"

            tz_name = self._track_tz(course_name, country)
            local_tz = ZoneInfo(tz_name)
            local_dt = datetime.combine(dt_obj.date(), datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=local_tz)
            utc_dt = local_dt.astimezone(ZoneInfo("UTC"))

            return RaceData(
                id=self._race_id(course_name, date_str, race_time),
                course=course_name,
                race_time=race_time,
                utc_datetime=utc_dt,
                local_time=local_dt.strftime("%H:%M"),
                timezone_name=tz_name,
                field_size=field_size,
                country=country,
                discipline="thoroughbred",
                race_number=None,
                grade=None,
                distance=None,
                surface=None,
                favorite=None,
                second_favorite=None,
                all_runners=[{"name": f"Runner {i+1}", "odds_str": ""} for i in range(field_size)],
                race_url=race_url,
                data_sources={"course": "SkySports", "runners": "SkySports"},
            )
        except Exception as e:
            logging.debug(f"SkySports parse error on container: {e}")
            return None

class GBGreyhoundSource(DataSourceBase):
    BASE = "https://www.sportinglife.com"
    INDEX = f"{BASE}/greyhounds/racecards"

    async def fetch_races(self, date_range: Tuple[datetime, datetime]) -> List[RaceData]:
        html = await self.http_client.fetch(self.INDEX)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        meeting_links = set()
        for a in soup.find_all("a", href=re.compile(r"^/greyhounds/racecards/[a-z0-9\-]+/\d{4}-\d{2}-\d{2}$")):
            meeting_links.add(urljoin(self.BASE, a["href"]))

        tasks = [self._fetch_meeting(m_url) for m_url in sorted(list(meeting_links))]
        results = await asyncio.gather(*tasks)
        races = [race for sublist in results for race in sublist]
        logging.info(f"SportingLife greyhound total races for window: {len(races)} across {len(meeting_links)} meetings")
        return races

    async def _fetch_meeting(self, m_url: str) -> List[RaceData]:
        html = await self.http_client.fetch(m_url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        race_links = set()
        for a in soup.find_all("a", href=re.compile(r"^/greyhounds/racecards/[a-z0-9\-]+/\d{4}-\d{2}-\d{2}/\d{3,4}$")):
            race_links.add(urljoin(self.BASE, a["href"]))

        tasks = [self._parse_race(r_url) for r_url in sorted(list(race_links))]
        results = await asyncio.gather(*tasks)
        return [race for race in results if race]

    async def _parse_race(self, url: str) -> Optional[RaceData]:
        html = await self.http_client.fetch(url)
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        if not (m := re.search(r"/([a-z0-9\-]+)/([\d\-]+)/\d{3,4}$", url)):
            return None
        course_slug, date_str = m.group(1), m.group(2)
        hhmm_match = re.search(r"/(\d{3,4})$", url)
        if not hhmm_match:
            return None
        hhmm = hhmm_match.group(1)
        course = course_slug.replace("-", " ").title()
        race_time = f"{'0' if len(hhmm) == 3 else ''}{hhmm}"
        race_time = f"{race_time[:2]}:{race_time[2:]}"
        runners = [
            {
                "name": tds[1].get_text(strip=True),
                "odds_str": tds[-1].get_text(strip=True) if len(tds) >= 3 else "",
            }
            for tr in soup.find_all("tr")
            if (tds := tr.find_all(["td", "th"])) and len(tds) > 1 and tds[1].get_text(strip=True)
        ]
        if not runners:
            return None
        fav_sorted = sorted(runners, key=lambda x: convert_odds_to_fractional(x.get("odds_str", "")))
        tz_name = self._track_tz(course, "GB")
        local_tz = ZoneInfo(tz_name)
        local_dt = datetime.combine(datetime.fromisoformat(date_str).date(), datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=local_tz)
        return RaceData(
            id=self._race_id(course, date_str, race_time),
            course=course,
            race_time=race_time,
            utc_datetime=local_dt.astimezone(ZoneInfo("UTC")),
            local_time=local_dt.strftime("%H:%M"),
            timezone_name=tz_name,
            field_size=len(runners),
            country="GB",
            discipline="greyhound",
            race_number=None,
            grade=None,
            distance=None,
            surface=None,
            favorite=fav_sorted[0] if fav_sorted else None,
            second_favorite=fav_sorted[1] if len(fav_sorted) > 1 else None,
            all_runners=runners,
            race_url=url,
            data_sources={"course": "SportingLife", "runners": "SportingLife", "odds": "SportingLife"},
        )

class HarnessRacingAustraliaSource(DataSourceBase):
    BASE = "https://www.harness.org.au"

    async def fetch_races(self, date_range: Tuple[datetime, datetime]) -> List[RaceData]:
        out: List[RaceData] = []
        for dt in self._days(date_range):
            index_url = f"{self.BASE}/racing/fields/?firstDate={dt.strftime('%d/%m/%Y')}"
            html = await self.http_client.fetch(index_url)
            if not html:
                continue
            soup = BeautifulSoup(html, "html.parser")
            race_links = set()
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not re.search(r"/racing/fields/race-fields", href):
                    continue
                href = href.split('#', 1)[0]
                race_links.add(urljoin(self.BASE, href))
            tasks = [self._parse_race(r_url, dt) for r_url in sorted(list(race_links))]
            results = await asyncio.gather(*tasks)
            out.extend([r for r in results if r])
        logging.info(f"HarnessRacingAustralia total races for window: {len(out)}")
        return out

    async def _parse_race(self, url: str, dt: datetime) -> Optional[RaceData]:
        # PATCHED: Harden HRA fetches with headless fallback second chance
        html = await self.http_client.fetch(url) or await self.http_client.fetch(url, use_browser=True)
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        course_h = soup.find(["h1", "h2"])
        course = course_h.get_text(strip=True) if course_h else "Harness Meeting"
        if not (race_time := parse_local_hhmm(soup.get_text(" ", strip=True))):
            return None

        # Hardened parsing: Only accept rows from proper field tables; no generic LI fallback
        runners_table = soup.find("table", class_=re.compile("field|runner|accept", re.I))
        runners: List[str] = []
        if runners_table:
            for tr in runners_table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) > 1:
                    name = tds[1].get_text(strip=True)
                    if name:
                        runners.append(name)
        # Cap implausible sizes
        if len(runners) < 3 or len(runners) > 16:
            return None

        runners_data = [{"name": r, "odds_str": ""} for r in runners]

        # Derive correct meeting date from mc parameter when present
        date_str = dt.strftime("%Y-%m-%d")
        try:
            m_mc = re.search(r"[?&]mc=([A-Z]{2})(\d{2})(\d{2})(\d{2})", url)
            if m_mc:
                _, dd, mm, yy = m_mc.groups()
                date_str = f"20{yy}-{mm}-{dd}"
        except Exception:
            pass

        tz_name = self._track_tz(course, "AU")
        local_tz = ZoneInfo(tz_name)
        try:
            local_date = datetime.fromisoformat(date_str).date()
        except Exception:
            local_date = dt.date()
        local_dt = datetime.combine(local_date, datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=local_tz)
        return RaceData(
            id=self._race_id(course, date_str, race_time),
            course=course,
            race_time=race_time,
            utc_datetime=local_dt.astimezone(ZoneInfo("UTC")),
            local_time=local_dt.strftime("%H:%M"),
            timezone_name=tz_name,
            field_size=len(runners_data),
            country="AU",
            discipline="harness",
            race_number=None,
            grade=None,
            distance=None,
            surface=None,
            favorite=None,
            second_favorite=None,
            all_runners=runners_data,
            race_url=url,
            data_sources={"course": "HRA", "runners": "HRA"},
        )

class StandardbredCanadaSource(DataSourceBase):
    BASE = "https://standardbredcanada.ca"

    async def fetch_races(self, date_range: Tuple[datetime, datetime]) -> List[RaceData]:
        out: List[RaceData] = []
        for dt in self._days(date_range):
            # PATCHED: Standardbred Canada index fallback (handles 404 dates)
            index_candidates = [
                f"{self.BASE}/racing/entries/date/{dt.strftime('%Y-%m-%d')}",
                f"{self.BASE}/racing/entries?date={dt.strftime('%Y-%m-%d')}",
                f"{self.BASE}/racing/entries",
            ]
            html = None
            for idx in index_candidates:
                html = await self.http_client.fetch(idx)
                if html:
                    break
            if not html:
                continue

            soup = BeautifulSoup(html, "html.parser")
            # prefer matching date links; if none, accept others with that date in anchor text
            meeting_links = set()
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.match(rf"^/racing/entries/[a-z0-9\-]+/{dt.strftime('%Y-%m-%d')}$", href):
                    meeting_links.add(urljoin(self.BASE, href))
            if not meeting_links:
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if re.match(r"^/racing/entries/[a-z0-9\-]+/\d{4}-\d{2}-\d{2}$", href) and dt.strftime("%Y-%m-%d") in a.get_text(" ", strip=True):
                        meeting_links.add(urljoin(self.BASE, href))

            tasks = [self._parse_meeting(m_url, dt) for m_url in sorted(list(meeting_links))]
            results = await asyncio.gather(*tasks)
            out.extend([r for sublist in results for r in sublist])
        logging.info(f"StandardbredCanada total races for window: {len(out)}")
        return out

    async def _parse_meeting(self, url: str, dt: datetime) -> List[RaceData]:
        html = await self.http_client.fetch(url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        races: List[RaceData] = []
        # Hardened parsing: process distinct race sections
        race_sections = soup.find_all(["section", "div"], id=re.compile(r"race-\d+", re.I))
        if not race_sections:
            race_sections = [soup]
        for sec in race_sections:
            if rd := self._parse_race_section(sec, url, dt):
                races.append(rd)
        return races

    def _parse_race_section(self, sec_soup: BeautifulSoup, page_url: str, dt: datetime) -> Optional[RaceData]:
        course_h = sec_soup.find_parent("main").find(["h1", "h2"]) if sec_soup.find_parent("main") else sec_soup.find(["h1", "h2"])
        course = course_h.get_text(strip=True) if course_h else "Standardbred Canada"
        if not (race_time := parse_local_hhmm(sec_soup.get_text(" ", strip=True))):
            return None

        runners_table = sec_soup.find("table", class_=re.compile("entries", re.I))
        if runners_table:
            runners = [
                tds[1].get_text(strip=True)
                for tr in runners_table.find_all("tr")
                if (tds := tr.find_all("td")) and len(tds) > 2 and tds[1].get_text(strip=True)
            ]
        else:
            return None

        runners_data = [{"name": r, "odds_str": ""} for r in runners]
        if not runners_data:
            return None

        tz_name = self._track_tz(course, "CA")
        local_tz = ZoneInfo(tz_name)
        date_str = dt.strftime("%Y-%m-%d")
        local_dt = datetime.combine(dt.date(), datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=local_tz)
        return RaceData(
            id=self._race_id(course, date_str, race_time),
            course=course,
            race_time=race_time,
            utc_datetime=local_dt.astimezone(ZoneInfo("UTC")),
            local_time=local_dt.strftime("%H:%M"),
            timezone_name=tz_name,
            field_size=len(runners_data),
            country="CA",
            discipline="harness",
            race_number=None,
            grade=None,
            distance=None,
            surface=None,
            favorite=None,
            second_favorite=None,
            all_runners=runners_data,
            race_url=page_url,
            data_sources={"course": "StdCan", "runners": "StdCan"},
        )

class RacingDataAggregator:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.cache = None if cfg.get("no_cache") else CacheManager(Path(cfg.get("cache_dir", DEFAULT_CACHE_DIR)))
        self.http = AsyncHttpClient(
            cfg.get("concurrency", 12), self.cache, not cfg.get("insecure_ssl", False), not cfg.get("no_http2", False)
        )
        self.scorer = ValueScorer(cfg.get("thresholds", {}))
        self.sources = [
            AtTheRacesSource(self.http),
            SportingLifeHorseApiSource(self.http),
            SkySportsSource(self.http),
            GBGreyhoundSource(self.http),
            HarnessRacingAustraliaSource(self.http),
            StandardbredCanadaSource(self.http),
        ]
        self.per_source_counts: Dict[str, int] = {}

    async def aclose(self):
        await self.http.aclose()

    async def fetch_all(self, start: datetime, end: datetime) -> List[RaceData]:
        tasks = [src.fetch_races((start, end)) for src in self.sources]
        t0 = time.perf_counter()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        races: List[RaceData] = []
        for src, res in zip(self.sources, results):
            if isinstance(res, Exception):
                logging.error(f"Source {src.name} failed: {res}")
                self.per_source_counts[src.name] = 0
            else:
                self.per_source_counts[src.name] = len(res)
                logging.info(f"Fetched {len(res)} races from {src.name}")
                races.extend(res)
        total_raw = len(races)
        logging.info(f"Total raw races before merge: {total_raw} from {len(self.sources)} sources; breakdown: {self.per_source_counts}")
        merged = self._dedupe_merge(races)
        total_merged = len(merged)
        logging.info(f"After dedupe/merge: {total_merged} races (deduped {total_raw - total_merged})")
        await self._enrich_rs_links(merged)
        logging.info("Enrichment with Racing & Sports links completed")
        for r in merged:
            r.value_score = self.scorer.calculate_score(r)
        logging.info(f"Scoring completed for {len(merged)} races in {time.perf_counter() - t0:.2f}s")
        return merged

    # PATCHED: Dedup key correctness (avoid over-merge) - use race_time only, rounded to 5m
    def _key(self, race: RaceData) -> str:
        course = self._normalize_course_name(race.course)
        date = race.utc_datetime.strftime("%Y-%m-%d")
        try:
            t = datetime.strptime(race.race_time, "%H:%M")
            t_norm = t.replace(minute=(t.minute // 5) * 5, second=0).strftime("%H:%M")
        except Exception:
            t_norm = race.race_time
        return "|".join([course, date, t_norm])

    def _normalize_course_name(self, name: str) -> str:
        if not name:
            return ""
        return re.sub(r"\s*\([^)]*\)", "", name.lower().strip())

    def _merge(self, a: RaceData, b: RaceData) -> RaceData:
        # Prioritize the data source with richer info (odds > runners > basic)
        def get_richness(r: RaceData):
            if any(runner.get("odds_str") for runner in r.all_runners):
                return 2
            if r.field_size > 0:
                return 1
            return 0

        primary, secondary = (a, b) if get_richness(a) >= get_richness(b) else (b, a)

        merged_sources = {**secondary.data_sources, **primary.data_sources}
        all_runners = primary.all_runners if len(primary.all_runners) >= len(secondary.all_runners) else secondary.all_runners

        fav, sec_fav = primary.favorite, primary.second_favorite
        if not fav and all_runners:
            fav_sorted = sorted(all_runners, key=lambda x: convert_odds_to_fractional(x.get("odds_str", "")))
            fav = fav_sorted[0] if fav_sorted else None
            sec_fav = fav_sorted[1] if len(fav_sorted) > 1 else None

        return RaceData(
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
            favorite=fav,
            second_favorite=sec_fav,
            all_runners=all_runners,
            race_url=primary.race_url or secondary.race_url,
            form_guide_url=primary.form_guide_url or secondary.form_guide_url,
            data_sources=merged_sources,
        )

    def _dedupe_merge(self, races: List[RaceData]) -> List[RaceData]:
        m: Dict[str, RaceData] = {}
        for r in races:
            k = self._key(r)
            m[k] = self._merge(m[k], r) if k in m else r
        return list(m.values())

    async def _enrich_rs_links(self, races: List[RaceData]) -> None:
        url = "https://www.racingandsports.com.au/todays-racing-json-v2"
        try:
            if not (text := await self.http.fetch(url)):
                return
            payload = json.loads(text)
            lookup: Dict[Tuple[str, str], str] = {}
            for disc in payload or []:
                for country in disc.get("Countries", []):
                    for meet in country.get("Meetings", []):
                        course = meet.get("Course")
                        link = meet.get("PDFUrl") or meet.get("PreMeetingUrl")
                        if not course or not link:
                            continue
                        if not (m := re.search(r"/(\d{4}-\d{2}-\d{2})", link)):
                            continue
                        lookup[(self._norm_course(course), m.group(1))] = link
            for r in races:
                date = r.utc_datetime.astimezone(ZoneInfo(r.timezone_name)).strftime("%Y-%m-%d")
                key = (self._norm_course(r.course), date)
                if (link := lookup.get(key)) and not r.form_guide_url:
                    r.form_guide_url = link
                    r.data_sources["form"] = "R&S"
        except Exception as e:
            logging.debug(f"R&S enrichment failed: {e}")

    def _norm_course(self, name: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"\s*\([^)]*\)", "", name.lower().strip()).replace("-", " "))

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
                "id",
                "course",
                "country",
                "discipline",
                "race_time",
                "local_time",
                "timezone_name",
                "field_size",
                "value_score",
                "favorite_name",
                "favorite_odds",
                "second_favorite_name",
                "second_favorite_odds",
                "race_url",
                "form_guide_url",
                "data_sources",
            ]
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                for r in races:
                    fav = r.favorite or {}
                    sec = r.second_favorite or {}
                    w.writerow(
                        {
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
                            "data_sources": ",".join(r.data_sources.values()),
                        }
                    )

        await asyncio.to_thread(_write)
        logging.info(f"Wrote CSV {fname}")

    def _filter_races(self, races: List[RaceData]) -> List[RaceData]:
        min_f = self.th.get("min_field_size", 4)
        max_f = self.th.get("max_field_size", 6)
        return [r for r in races if min_f <= r.field_size <= max_f]

    def _generate_html(self, all_races: List[RaceData], stats: Dict[str, Any]) -> str:
        filtered_races = self._filter_races(all_races)
        filtered_races.sort(key=lambda r: (r.field_size, r.utc_datetime))
        all_races.sort(key=lambda r: r.value_score, reverse=True)

        icons = {"thoroughbred": "🐎", "greyhound": "🐕", "harness": "👨‍🦽"}
        strategies = {3: "[Tri]", 4: "[Superfecta]", 5: "[Superfecta+]", 6: "[Superfecta++]"}

        style_and_script = """
        <style>:root{color-scheme:light dark}body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;background-color:#f4f4f9;color:#333;margin:0;padding:20px;line-height:1.6}.container{max-width:1200px;margin:auto;background-color:#fff;padding:20px;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,.05)}h1{color:#1a1a1a;border-bottom:3px solid #007bff;padding-bottom:10px}.summary{background:#e7f3ff;padding:15px;border-radius:8px;margin-bottom:20px;border-left:4px solid #007bff}.tabs{display:flex;border-bottom:2px solid #dee2e6;margin-bottom:20px}.tab{padding:10px 20px;cursor:pointer;background:#f8f9fa;border:1px solid #dee2e6;border-bottom:none;margin-right:2px;border-radius:4px 4px 0 0}.tab.active{background:#007bff;color:white;border-color:#007bff}.tab-content{display:none}.tab-content.active{display:block}.race-group h2{color:black;background-color:gold;padding:10px;text-align:center;border-radius:4px;margin-top:25px}.race{border:1px solid #ddd;padding:12px;margin:10px 0;border-radius:5px;background-color:#fafafa;border-left:5px solid #6c757d}.race.thoroughbred{border-left-color:#007bff}.race.harness{border-left-color:#17a2b8}.race.greyhound{border-left-color:#fd7e14}.head{display:flex;flex-wrap:wrap;align-items:center;gap:10px;font-weight:600;font-size:1.1em}.pill{display:inline-block;padding:3px 10px;border-radius:12px;background:#e9ecef;color:#495057;font-size:.85rem;font-weight:500}.links a{display:inline-block;text-decoration:none;background-color:#007bff;color:white;padding:6px 12px;border-radius:4px;margin-right:8px;font-size:.9em;margin-top:8px}.links a.alt{background:#28a745}.kv{font-size:.95rem;margin-top:8px;color:#555}.footer{text-align:center;margin-top:30px;font-size:.9em;color:#777}@media (prefers-color-scheme:dark){body{background-color:#121212;color:#e0e0e0}.container{background-color:#1e1e1e}h1{color:#e0e0e0;border-color:#4dabf7}.summary{background-color:#2c3e50;border-left-color:#4dabf7}.tabs{border-color:#444}.tab{background:#333;color:#ccc;border-color:#444}.tab.active{background:#4dabf7;color:#000;border-color:#4dabf7}.race{background-color:#2a2a2a;border-color:#444}.pill{background:#333;color:#ccc}.kv{color:#bbb}.links a{background-color:#4dabf7;color:#000}.links a.alt{background:#2ecc71}}</style>
        <script>function showTab(t){document.querySelectorAll(".tab-content").forEach(c=>c.classList.remove("active"));document.querySelectorAll(".tab").forEach(c=>c.classList.remove("active"));document.getElementById(t).classList.add("active");document.querySelector(`[onclick="showTab('${t}')"]`).classList.add("active")}document.addEventListener("DOMContentLoaded",()=>showTab("filtered"));</script>"""

        header = f"<h1>Global Racing Scanner Report V3.1</h1>"
        source_counts = stats.get('per_source_counts', {})
        summary_stats = (
            f"Found <b>{len(all_races)}</b> total races from <b>{len(source_counts)}</b> sources. "
            f"<b>{len(filtered_races)}</b> races match filter. Runtime: <b>{stats.get('duration_seconds',0):.1f}s</b>."
        )
        summary = f"<div class='summary'>{summary_stats}</div>"
        tabs = (
            f"""
            <div class="tabs">
              <div class="tab" onclick="showTab('filtered')">🎯 Filtered Races ({len(filtered_races)})</div>
              <div class="tab" onclick="showTab('all')">⭐ All Races by Value ({len(all_races)})</div>
            </div>
            """
        )

        filtered_content = "<div id='filtered' class='tab-content'>"
        if not filtered_races:
            filtered_content += "<p>No races found matching the filter criteria.</p>"
        else:
            last_field_size = None
            for r in filtered_races:
                if r.field_size != last_field_size:
                    filtered_content += (
                        f"<div class='race-group'><h2>{r.field_size} Runners {strategies.get(r.field_size, '')}</h2></div>"
                    )
                    last_field_size = r.field_size
                filtered_content += self._render_race_block(r, icons)
        filtered_content += "</div>"

        all_content = "<div id='all' class='tab-content'>"
        if not all_races:
            all_content += "<p>No races found from any source.</p>"
        else:
            for r in all_races:
                all_content += self._render_race_block(r, icons)
        all_content += "</div>"

        footer = f"<div class='footer'>Generated {datetime.now().isoformat(timespec='seconds')}</div>"
        return (
            f"<!doctype html><html><head><meta charset='utf-8'><title>Global Racing Report</title>"
            f"{style_and_script}</head><body><div class='container'>{header}{summary}{tabs}{filtered_content}{all_content}{footer}</div></body></html>"
        )

    def _render_race_block(self, r: RaceData, icons: Dict) -> str:
        icon = icons.get(r.discipline, "❓")
        fav = r.favorite or {}
        sec = r.second_favorite or {}
        links = [f"<a href='{r.race_url}' target='_blank' rel='noopener'>Racecard</a>"]
        if r.form_guide_url:
            links.append(
                f"<a class='alt' href='{r.form_guide_url}' target='_blank' rel='noopener'>Form</a>"
            )
        source_tags = "".join([f"<span class='pill'>{s.upper()}</span>" for s in set(r.data_sources.values())])
        return (
            f"<div class='race {r.discipline}'>"
            f"<div class='head'>{icon} {r.course} ({r.country})"
            f"<span class='pill'>{r.local_time} {r.timezone_name}</span>"
            f"<span class='pill'>Field {r.field_size}</span>"
            f"<span class='pill'>Score {r.value_score:.1f}</span></div>"
            f"<div class='kv'><b>Fav:</b> {fav.get('name','N/A')} ({fav.get('odds_str','N/A')}) &nbsp; "
            f"<b>2nd Fav:</b> {sec.get('name','N/A')} ({sec.get('odds_str','N/A')})</div>"
            f"<div class='kv'><b>Sources:</b> {source_tags}</div>"
            f"<div class='links'>{''.join(links)}</div>"
            f"</div>"
        )

    async def _write_html(self, races: List[RaceData], stats: Dict[str, Any], fname: str):
        html = self._generate_html(races, stats)
        async with aiofiles.open(self.out_dir / fname, "w", encoding="utf-8") as f:
            await f.write(html)
        logging.info(f"Wrote HTML {fname}")

    def _race_to_dict(self, r: RaceData) -> Dict[str, Any]:
        d = asdict(r)
        d["utc_datetime"] = r.utc_datetime.isoformat()
        return d

async def _amain(args):
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s: %(message)s", force=True)
    if args.insecure_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    if args.disable_browser_fetch:
        os.environ["DISABLE_BROWSER_FETCH"] = "1"
    
    start_dt = datetime.now() - timedelta(days=args.days_back)
    end_dt = datetime.now() + timedelta(days=args.days_forward)

    thresholds = {
        "min_field_size": args.min_field_size,
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
    # Manual content injection: preload cache entries from files or stdin
    try:
        if getattr(args, "manual_kv", None):
            for kv in args.manual_kv:
                if "=" not in kv:
                    logging.warning(f"--manual-kv entry malformed (expected url=path): {kv}")
                    continue
                url, path = kv.split("=", 1)
                try:
                    content = Path(path).read_text(encoding="utf-8", errors="ignore")
                    if agg.cache:
                        await agg.cache.set(url, content, headers={"cache-control": "max-age=3600"})
                        logging.info(f"Preloaded manual content for {url} from {path}")
                except Exception as e:
                    logging.error(f"Failed to load manual file for {url}: {e}")
        if getattr(args, "manual_stdin_url", None):
            pasted = sys.stdin.read()
            if pasted and agg.cache:
                await agg.cache.set(args.manual_stdin_url, pasted, headers={"cache-control": "max-age=3600"})
                logging.info(f"Preloaded manual content for {args.manual_stdin_url} from stdin")
    except Exception as e:
        logging.debug(f"Manual injection error: {e}")
    try:
        races = await agg.fetch_all(start_dt, end_dt)
    finally:
        await agg.aclose()
    duration = time.perf_counter() - t0
    stats = {
        "cache_hits": agg.cache.hit_count if agg.cache else 0,
        "cache_misses": agg.cache.miss_count if agg.cache else 0,
        "http_success": agg.http.success_count,
        "http_retries": agg.http.retry_count,
        "http_blocked": agg.http.blocked_count,
        "fallback_success": agg.http.fallback_success,
        "browser_attempts": agg.http.browser_attempts,
        "browser_success": agg.http.browser_success,
        "races_total": len(races),
        "duration_seconds": duration,
        "per_source_counts": getattr(agg, "per_source_counts", {}),
    }
    out = OutputManager(Path(args.output_dir), cfg, thresholds)
    await out.write_all(races, stats, args.formats)

def parse_args():
    p = argparse.ArgumentParser(description="Melt & Repour Global Racing Scanner V3.1 (Single File)")
    p.add_argument("--days-back", type=int, default=0, help="Days back to scan (0=today only)")
    p.add_argument("--days-forward", type=int, default=1, help="Days forward to scan (1=includes tomorrow)")
    p.add_argument("--concurrency", type=int, default=12, help="Max concurrent HTTP requests")
    p.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR), help="Cache directory")
    p.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory")
    p.add_argument("--formats", nargs="+", default=["html", "json"], help="Output formats")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    p.add_argument("--min-field-size", type=int, default=4, help="Min field size for 'Filtered Races'")
    p.add_argument("--max-field-size", type=int, default=6, help="Max field size for 'Filtered Races'")
    p.add_argument("--min-fav-fractional", type=float, default=1.0)
    p.add_argument("--min-second-fav-fractional", type=float, default=3.0)
    p.add_argument("--min-odds-ratio", type=float, default=0.0)
    p.add_argument("--no-cache", action="store_true", help="Disable on-disk cache")
    p.add_argument("--insecure-ssl", action="store_true", help="Disable SSL verification")
    p.add_argument("--no-http2", action="store_true", help="Disable HTTP/2")
    p.add_argument("--disable-browser-fetch", action="store_true", help="Disable browser fallback")
    # Manual content injection options
    p.add_argument("--manual-kv", nargs="*", help="Preload cache with url=path pairs (HTML/JSON file contents)")
    p.add_argument("--manual-stdin-url", help="URL to associate with content read from stdin (paste then Ctrl-D)")
    return p.parse_args()

def main():
    args = parse_args()
    try:
        asyncio.run(_amain(args))
        print("\n✅ Scan complete!")
        if "html" in args.formats:
            try:
                output_path = Path(args.output_dir)
                html_files = sorted(output_path.glob("*.html"), key=os.path.getmtime, reverse=True)
                if html_files:
                    import webbrowser
                    webbrowser.open(f'file://{os.path.abspath(html_files[0])}')
                    print(f"🌐 Opened report: {html_files[0].name}")
            except Exception as e:
                print(f"⚠️ Could not auto-open report in browser: {e}")
    except KeyboardInterrupt:
        print("\n\n⚠️ Scan interrupted by user.")
    except Exception as e:
        logging.error(f"A critical error occurred: {e}", exc_info=args.debug)
        print("\n❌ A critical error occurred. Run with --debug for more information.")

if __name__ == "__main__":
    main()