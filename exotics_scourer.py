#!/usr/bin/env python3

import argparse
import json
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple

import requests
from bs4 import BeautifulSoup
from curl_cffi.requests import Session as CurlCffiSession


# ----------------- Shared networking -----------------

@dataclass
class RunnerRow:
    pos: Optional[str]
    horse: str
    sp_str: Optional[str]
    sp_fractional: Optional[float]


@dataclass
class ExoticRace:
    date: str
    course: str
    time: str
    url: str
    field_size: int
    favorite_fractional: Optional[float]
    second_favorite_fractional: Optional[float]
    favorite_name: Optional[str]
    second_favorite_name: Optional[str]
    csf: Optional[str] = None
    tricast: Optional[str] = None
    exacta: Optional[str] = None
    trifecta: Optional[str] = None
    superfecta: Optional[str] = None


def fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20, verify=False)
        r.raise_for_status()
        return r.text
    except Exception:
        pass
    try:
        sess = CurlCffiSession(impersonate="chrome110")
        r = sess.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        r.raise_for_status()
        return r.text
    except Exception:
        return None


def convert_odds_to_fractional(odds_str: Optional[str]) -> Optional[float]:
    if not odds_str:
        return None
    s = odds_str.strip().upper().replace('-', '/').replace(' ', '')
    if s in ('SP', ''):
        return None
    if s == 'EVS' or s == 'Evens':
        return 1.0
    if '/' in s:
        try:
            num, den = s.split('/', 1)
            num = float(re.sub(r'[^0-9.]', '', num))
            den = float(re.sub(r'[^0-9.]', '', den))
            if den == 0:
                return None
            return num / den
        except Exception:
            return None
    # decimal odds -> fractional by (decimal - 1)
    try:
        dec = float(re.sub(r'[^0-9.]', '', s))
        if dec > 1:
            return dec - 1.0
    except Exception:
        return None
    return None


EXOTIC_REGEX = re.compile(r"(CSF|Tricast|Trifecta|Exacta|Superfecta|Tote Exacta|Tote Trifecta)\s*[:\-]?\s*([£$€]?[\d,.]+)", re.IGNORECASE)


def parse_exotics_from_html(html: str) -> Dict[str, Optional[str]]:
    res = {"csf": None, "tricast": None, "exacta": None, "trifecta": None, "superfecta": None}
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(' ', strip=True)
    for m in EXOTIC_REGEX.finditer(text):
        label = m.group(1).lower()
        val = m.group(2)
        if 'csf' in label:
            res['csf'] = val
        elif 'tricast' in label and 'trifecta' not in label:
            res['tricast'] = val
        elif 'trifecta' in label:
            res['trifecta'] = val
        elif 'exacta' in label:
            res['exacta'] = val
        elif 'superfecta' in label:
            res['superfecta'] = val
    return res


# ----------------- Sky Sports (best-effort) -----------------

def list_meeting_links_for_date(date_obj: datetime) -> List[str]:
    url = f"https://www.skysports.com/racing/results/{date_obj.strftime('%Y-%m-%d')}"
    html = fetch_html(url)
    if not html:
        return []
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.startswith('/racing/results/') and href.count('/') == 5:
            # format: /racing/results/YYYY-MM-DD/{course-slug}
            links.append('https://www.skysports.com' + href)
    return sorted(list(set(links)))


def list_race_links_from_meeting(meeting_url: str) -> List[str]:
    html = fetch_html(meeting_url)
    if not html:
        return []
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.startswith('/racing/results/') and re.search(r'/\d{4}$', href):
            links.append('https://www.skysports.com' + href)
    return sorted(list(set(links)))


def parse_race_page(url: str) -> Optional[ExoticRace]:
    html = fetch_html(url)
    if not html:
        return None
    soup = BeautifulSoup(html, 'html.parser')

    # Extract date, course, time from URL or header
    m = re.search(r'/racing/results/(\d{4}-\d{2}-\d{2})/([^/]+)/([0-9]{4})$', url)
    if not m:
        # try to recover from header
        return None
    date_str, course_slug, hhmm = m.groups()
    course = course_slug.replace('-', ' ').title()
    time_str = f"{hhmm[:2]}:{hhmm[2:]}"

    # Parse runners with odds from result table
    runners: List[RunnerRow] = []
    # Attempt: find rows under a table that likely contains result positions
    for tr in soup.find_all('tr'):
        tds = tr.find_all(['td', 'th'])
        if len(tds) < 3:
            continue
        pos_text = tds[0].get_text(strip=True)
        horse_text = tds[1].get_text(strip=True)
        sp_text = tds[-1].get_text(strip=True)
        # Heuristic: position starts with a digit and horse name non-empty
        if re.match(r'^(PU|UR|F|BD|RO|\d+)[a-zA-Z]*$', pos_text) and horse_text:
            frac = convert_odds_to_fractional(sp_text)
            runners.append(RunnerRow(pos=pos_text, horse=horse_text, sp_str=sp_text, sp_fractional=frac))

    # Fallback field size from count
    field_size = len([r for r in runners if r.pos and re.match(r'^\d+$', r.pos)]) or len(runners)

    if not runners:
        return ExoticRace(
            date=date_str, course=course, time=time_str, url=url,
            field_size=0, favorite_fractional=None, second_favorite_fractional=None,
            favorite_name=None, second_favorite_name=None,
        )

    # Determine favorite and second favorite by lowest fractional odds
    runners_with_odds = [r for r in runners if r.sp_fractional is not None]
    runners_with_odds.sort(key=lambda r: r.sp_fractional)
    favorite = runners_with_odds[0] if runners_with_odds else None
    second_fav = runners_with_odds[1] if len(runners_with_odds) > 1 else None

    # Extract exotics
    ex = parse_exotics_from_html(html)

    return ExoticRace(
        date=date_str,
        course=course,
        time=time_str,
        url=url,
        field_size=field_size,
        favorite_fractional=favorite.sp_fractional if favorite else None,
        second_favorite_fractional=second_fav.sp_fractional if second_fav else None,
        favorite_name=favorite.horse if favorite else None,
        second_favorite_name=second_fav.horse if second_fav else None,
        csf=ex.get('csf'), tricast=ex.get('tricast'), exacta=ex.get('exacta'),
        trifecta=ex.get('trifecta'), superfecta=ex.get('superfecta'),
    )


# ----------------- At The Races -----------------

def atr_slug_course(name: str) -> str:
    return re.sub(r"\s+", "-", name.strip().lower())


def atr_list_meetings_for_date(date_obj: datetime) -> List[str]:
    url = f"https://www.attheraces.com/results/{date_obj.strftime('%Y-%m-%d')}"
    html = fetch_html(url)
    if not html:
        return []
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        # Meeting link pattern: /results/YYYY-MM-DD/{course-slug}
        if re.match(r"^/results/\d{4}-\d{2}-\d{2}/[a-z0-9\-]+$", href):
            links.append('https://www.attheraces.com' + href)
    return sorted(list(set(links)))


def atr_list_race_result_links(meeting_url: str) -> List[str]:
    html = fetch_html(meeting_url)
    if not html:
        return []
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        # Race result link: /racecard/{course}/{YYYY-MM-DD}/{HHMM}/results
        if re.match(r"^/racecard/[a-z0-9\-]+/\d{4}-\d{2}-\d{2}/\d{4}/results$", href):
            links.append('https://www.attheraces.com' + href)
    return sorted(list(set(links)))


def atr_parse_race_result(url: str) -> Optional[ExoticRace]:
    html = fetch_html(url)
    if not html:
        return None
    soup = BeautifulSoup(html, 'html.parser')

    m = re.search(r"/racecard/([a-z0-9\-]+)/([0-9]{4}-[0-9]{2}-[0-9]{2})/([0-9]{4})/results$", url)
    if not m:
        return None
    course_slug, date_str, hhmm = m.groups()
    course = course_slug.replace('-', ' ').title()
    time_str = f"{hhmm[:2]}:{hhmm[2:]}"

    # Parse result table: find rows with positions and SPs
    runners: List[RunnerRow] = []
    for tr in soup.find_all('tr'):
        tds = tr.find_all(['td', 'th'])
        if len(tds) < 3:
            continue
        pos_text = tds[0].get_text(strip=True)
        horse_text = tds[1].get_text(strip=True)
        sp_text = tds[-1].get_text(strip=True)
        if horse_text and (re.match(r'^\d+$', pos_text) or pos_text in {'PU','UR','F','BD','RO'}):
            runners.append(RunnerRow(pos=pos_text, horse=horse_text, sp_str=sp_text, sp_fractional=convert_odds_to_fractional(sp_text)))

    field_size = len([r for r in runners if r.pos and re.match(r'^\d+$', r.pos)]) or len(runners)

    # Determine favorite and second favorite by lowest fractional odds
    runners_with_odds = [r for r in runners if r.sp_fractional is not None]
    runners_with_odds.sort(key=lambda r: r.sp_fractional)
    favorite = runners_with_odds[0] if runners_with_odds else None
    second_fav = runners_with_odds[1] if len(runners_with_odds) > 1 else None

    ex = parse_exotics_from_html(html)

    return ExoticRace(
        date=date_str,
        course=course,
        time=time_str,
        url=url,
        field_size=field_size,
        favorite_fractional=favorite.sp_fractional if favorite else None,
        second_favorite_fractional=second_fav.sp_fractional if second_fav else None,
        favorite_name=favorite.horse if favorite else None,
        second_favorite_name=second_fav.horse if second_fav else None,
        csf=ex.get('csf'), tricast=ex.get('tricast'), exacta=ex.get('exacta'),
        trifecta=ex.get('trifecta'), superfecta=ex.get('superfecta'),
    )


# ----------------- Scan orchestration -----------------

def scan_days(days_back: int, meeting_limit: Optional[int], race_limit: Optional[int], sleep_seconds: float) -> List[ExoticRace]:
    all_races: List[ExoticRace] = []
    today = datetime.utcnow().date()
    for i in range(days_back):
        d = today - timedelta(days=i)
        # Use ATR as primary source
        meeting_links = atr_list_meetings_for_date(datetime(d.year, d.month, d.day))
        if meeting_limit is not None:
            meeting_links = meeting_links[:meeting_limit]
        for m_url in meeting_links:
            race_links = atr_list_race_result_links(m_url)
            for r_url in race_links:
                if race_limit is not None and len(all_races) >= race_limit:
                    return all_races
                race = atr_parse_race_result(r_url)
                if race:
                    all_races.append(race)
                if sleep_seconds:
                    time.sleep(sleep_seconds)
    return all_races


def main():
    ap = argparse.ArgumentParser(description="Scour Sky Sports results for exotic payouts and filter on small fields, non-chalk favorites")
    ap.add_argument('--days-back', type=int, default=2, help='Number of days back from today to scan')
    ap.add_argument('--meeting-limit', type=int, default=None, help='Limit number of meetings per day')
    ap.add_argument('--race-limit', type=int, default=None, help='Global limit on number of races to parse')
    ap.add_argument('--sleep', type=float, default=0.25, help='Sleep seconds between requests')
    ap.add_argument('--max-field-size', type=int, default=7, help='Maximum field size to include')
    ap.add_argument('--min-fav-fractional', type=float, default=1.0, help='Minimum favorite fractional odds (>= EVS means not chalk)')
    ap.add_argument('--out-prefix', default='exotics_scour', help='Output prefix for JSON/CSV')
    args = ap.parse_args()

    races = scan_days(args.days_back, args.meeting_limit, args.race_limit, args.sleep)

    # Filter: small fields and favorite not chalk
    matches: List[ExoticRace] = []
    for r in races:
        if r.field_size and r.field_size <= args.max_field_size:
            if r.favorite_fractional is None or r.favorite_fractional >= args.min_fav_fractional:
                matches.append(r)

    # Output JSON
    payload = {
        'scanned': len(races),
        'matches': len(matches),
        'criteria': {
            'max_field_size': args.max_field_size,
            'min_fav_fractional': args.min_fav_fractional,
        },
        'data': [asdict(x) for x in matches]
    }
    with open(f"{args.out_prefix}.json", 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    # Output CSV
    import csv
    with open(f"{args.out_prefix}.csv", 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(['date','course','time','field_size','favorite','fav_frac','second_fav','sec_frac','csf','tricast','exacta','trifecta','superfecta','url'])
        for r in matches:
            w.writerow([
                r.date, r.course, r.time, r.field_size,
                r.favorite_name or '', f"{r.favorite_fractional:.2f}" if isinstance(r.favorite_fractional, float) else '',
                r.second_favorite_name or '', f"{r.second_favorite_fractional:.2f}" if isinstance(r.second_favorite_fractional, float) else '',
                r.csf or '', r.tricast or '', r.exacta or '', r.trifecta or '', r.superfecta or '', r.url
            ])

    print(f"Scanned races: {len(races)} | Matches: {len(matches)}")
    print(f"Wrote: {args.out_prefix}.json and {args.out_prefix}.csv")


if __name__ == '__main__':
    main()