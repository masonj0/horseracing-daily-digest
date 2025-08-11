#!/usr/bin/env python3

import argparse
import csv
import json
import math
import os
import re
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from glob import glob
from typing import List, Optional, Dict, Any, Tuple

import requests
from bs4 import BeautifulSoup
from curl_cffi.requests import Session as CurlCffiSession


@dataclass
class RunnerResult:
    horse_name: str
    fav_label: Optional[str]
    sp_from: Optional[int]
    sp_to: Optional[int]
    sp_fractional: Optional[float]
    finishing_pos: Optional[str]


@dataclass
class RaceResult:
    source_file: str
    course: str
    date_str: str
    time_str: str
    race_desc: str
    field_size: int
    winner: Optional[RunnerResult]
    favorite: Optional[RunnerResult]
    second_favorite: Optional[RunnerResult]
    winner_return_on_1: Optional[float]
    fav_fractional: Optional[float]
    second_fav_fractional: Optional[float]
    odds_ratio_second_over_fav: Optional[float]
    # Exotic payouts (best-effort; currency-preserving strings)
    csf: Optional[str] = None
    tricast: Optional[str] = None
    exacta: Optional[str] = None
    trifecta: Optional[str] = None
    superfecta: Optional[str] = None


# --------------------------- Helpers ---------------------------

def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        s = str(value).strip().replace(",", "")
        if s == "":
            return None
        return int(s)
    except Exception:
        return None


def compute_fractional(sp_from: Optional[int], sp_to: Optional[int]) -> Optional[float]:
    if sp_from is None or sp_to is None:
        return None
    if sp_to == 0:
        return None
    try:
        return float(sp_from) / float(sp_to)
    except Exception:
        return None


def parse_main_csv(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            normalized = { (k or "").strip(): (v or "").strip() for k, v in row.items() }
            rows.append(normalized)
    return rows


def parse_details_csv(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            normalized = { (k or "").strip(): (v or "").strip() for k, v in row.items() }
            rows.append(normalized)
    return rows


def build_runner(row: Dict[str, Any]) -> RunnerResult:
    sp_from = safe_int(row.get("SPFrom"))
    sp_to = safe_int(row.get("SPTo"))
    sp_frac = compute_fractional(sp_from, sp_to)
    return RunnerResult(
        horse_name=row.get("HorseName", "").strip(),
        fav_label=row.get("Favs", "").strip() or None,
        sp_from=sp_from,
        sp_to=sp_to,
        sp_fractional=sp_frac,
        finishing_pos=row.get("FPos", "").strip() or None,
    )


def parse_date(date_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(date_str.strip(), "%d-%b-%y")
    except Exception:
        return None


# --------------------------- Core processing ---------------------------

def process_pair(main_path: str, details_path: str) -> List[RaceResult]:
    main_rows = parse_main_csv(main_path)
    details_rows = parse_details_csv(details_path)

    details_by_main: Dict[str, List[Dict[str, Any]]] = {}
    for d in details_rows:
        mid = d.get("MainID", "").strip()
        details_by_main.setdefault(mid, []).append(d)

    results: List[RaceResult] = []

    for m in main_rows:
        main_id = m.get("Id", "").strip()
        course = m.get("Course", "").strip()
        date_s = m.get("Date", "").strip()
        time_s = m.get("Time", "").strip()
        race_desc = m.get("RaceDesc", "").strip()
        field_size = safe_int(m.get("Ran")) or 0

        runners_raw = details_by_main.get(main_id, [])
        runners: List[RunnerResult] = [build_runner(r) for r in runners_raw]

        winner: Optional[RunnerResult] = None
        for r in runners:
            if (r.finishing_pos or "").strip() in {"1", "1st"}:
                winner = r
                break

        favorite = next((r for r in runners if r.fav_label and r.fav_label.lower().startswith("fav")), None)
        second_favorite = next((r for r in runners if r.fav_label and r.fav_label.lower().startswith("2fav")), None)

        fav_frac = favorite.sp_fractional if favorite else None
        sec_frac = second_favorite.sp_fractional if second_favorite else None
        odds_ratio = None
        if fav_frac is not None and fav_frac > 0 and sec_frac is not None:
            odds_ratio = sec_frac / fav_frac

        winner_return = None
        if winner and winner.sp_fractional is not None:
            winner_return = winner.sp_fractional + 1.0

        results.append(RaceResult(
            source_file=os.path.basename(main_path),
            course=course,
            date_str=date_s,
            time_str=time_s,
            race_desc=race_desc,
            field_size=field_size,
            winner=winner,
            favorite=favorite,
            second_favorite=second_favorite,
            winner_return_on_1=winner_return,
            fav_fractional=fav_frac,
            second_fav_fractional=sec_frac,
            odds_ratio_second_over_fav=odds_ratio,
        ))

    return results


# --------------------------- Enrichment (Sky Sports) ---------------------------

def slugify_course_for_sky(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    return s


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


EXOTIC_LABELS = [
    "CSF", "Tricast", "Trifecta", "Exacta", "Superfecta", "Tote Exacta", "Tote Trifecta"
]

EXOTIC_REGEX = re.compile(r"(CSF|Tricast|Trifecta|Exacta|Superfecta|Tote Exacta|Tote Trifecta)\s*[:\-]?\s*([£$€]?[\d,.]+)", re.IGNORECASE)


def _parse_exotics_from_html_into(html: str, race: RaceResult) -> bool:
    soup_local = BeautifulSoup(html, "html.parser")
    found_local = False
    text_local = soup_local.get_text(" ", strip=True)
    for match in EXOTIC_REGEX.finditer(text_local):
        label = match.group(1).lower()
        amount = match.group(2)
        if "csf" in label:
            race.csf = amount
            found_local = True
        elif "tricast" in label and not "trifecta" in label:
            race.tricast = amount
            found_local = True
        elif "trifecta" in label:
            race.trifecta = amount
            found_local = True
        elif "exacta" in label:
            race.exacta = amount
            found_local = True
        elif "superfecta" in label:
            race.superfecta = amount
            found_local = True
    return found_local


def try_enrich_from_sky(race: RaceResult) -> bool:
    # Build Sky results URL: https://www.skysports.com/racing/results/YYYY-MM-DD/{course-slug}/{HHMM}
    dt = parse_date(race.date_str)
    if not dt:
        return False
    hhmm = re.sub(r":", "", race.time_str).strip()
    if not hhmm or not hhmm.isdigit():
        return False
    course_slug = slugify_course_for_sky(race.course)

    # Attempt 1: Direct race URL by hhmm
    url_direct = f"https://www.skysports.com/racing/results/{dt.strftime('%Y-%m-%d')}/{course_slug}/{hhmm}"
    html = fetch_html(url_direct)
    if html and _parse_exotics_from_html_into(html, race):
        return True

    # Attempt 2: Meeting page -> find the race link
    url_meeting = f"https://www.skysports.com/racing/results/{dt.strftime('%Y-%m-%d')}/{course_slug}"
    meeting_html = fetch_html(url_meeting)
    if not meeting_html:
        return False
    soup = BeautifulSoup(meeting_html, "html.parser")

    time_slug = hhmm
    candidate_link = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if f"/{time_slug}" in href and "/racing/results/" in href:
            candidate_link = href if href.startswith("http") else f"https://www.skysports.com{href}"
            break
    if candidate_link:
        page_html = fetch_html(candidate_link)
        if page_html and _parse_exotics_from_html_into(page_html, race):
            return True

    return False


def slugify_course_for_atr(name: str) -> str:
    return re.sub(r"\s+", "-", name.strip().lower())


def try_enrich_from_atr(race: RaceResult) -> bool:
    dt = parse_date(race.date_str)
    if not dt:
        return False
    hhmm = re.sub(r":", "", race.time_str).strip()
    if not hhmm or not hhmm.isdigit():
        return False
    course_slug = slugify_course_for_atr(race.course)
    url = f"https://www.attheraces.com/racecard/{course_slug}/{dt.strftime('%Y-%m-%d')}/{hhmm}/results"
    html = fetch_html(url)
    if not html:
        return False
    return _parse_exotics_from_html_into(html, race)


def enrich_exotics(races: List[RaceResult], max_to_fetch: Optional[int] = None, sleep_seconds: float = 0.5) -> int:
    enriched = 0
    count = 0
    for r in races:
        if max_to_fetch is not None and count >= max_to_fetch:
            break
        ok = try_enrich_from_sky(r)
        if not ok:
            ok = try_enrich_from_atr(r)
        if ok:
            enriched += 1
        count += 1
        if sleep_seconds:
            time.sleep(sleep_seconds)
    return enriched


# --------------------------- Filtering and outputs ---------------------------

def filter_races(races: List[RaceResult], max_field_size: int, min_fav_fractional: float) -> List[RaceResult]:
    filtered: List[RaceResult] = []
    for r in races:
        if r.field_size <= max_field_size:
            if r.fav_fractional is None or r.fav_fractional >= min_fav_fractional:
                filtered.append(r)
    return filtered


def write_outputs(races_all: List[RaceResult], races_filtered: List[RaceResult], out_prefix: str) -> None:
    payload = {
        "total_races": len(races_all),
        "filtered_races": len(races_filtered),
        "criteria": {
            "note": "small fields without chalk favorites",
        },
        "all": [asdict(r) for r in races_all],
        "matches": [asdict(r) for r in races_filtered],
    }
    json_path = f"{out_prefix}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    csv_path = f"{out_prefix}.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "source_file", "date", "time", "course", "race_desc", "field_size",
            "favorite", "fav_fractional", "second_favorite", "second_fav_fractional",
            "odds_ratio_second_over_fav", "winner", "winner_fractional", "winner_return_on_1",
            # exotic payouts
            "csf", "tricast", "exacta", "trifecta", "superfecta",
        ])
        for r in races_filtered:
            writer.writerow([
                r.source_file,
                r.date_str,
                r.time_str,
                r.course,
                r.race_desc,
                r.field_size,
                r.favorite.horse_name if r.favorite else "",
                f"{r.fav_fractional:.2f}" if isinstance(r.fav_fractional, float) else "",
                r.second_favorite.horse_name if r.second_favorite else "",
                f"{r.second_fav_fractional:.2f}" if isinstance(r.second_fav_fractional, float) else "",
                f"{r.odds_ratio_second_over_fav:.2f}" if isinstance(r.odds_ratio_second_over_fav, float) else "",
                r.winner.horse_name if r.winner else "",
                f"{r.winner.sp_fractional:.2f}" if (r.winner and isinstance(r.winner.sp_fractional, float)) else "",
                f"{r.winner_return_on_1:.2f}" if isinstance(r.winner_return_on_1, float) else "",
                r.csf or "",
                r.tricast or "",
                r.exacta or "",
                r.trifecta or "",
                r.superfecta or "",
            ])

    print(f"Wrote: {json_path} and {csv_path}")


# --------------------------- CLI ---------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Validate tipsheet against historical results with exotic payouts")
    parser.add_argument("--dir", default="/workspace/old_results", help="Directory containing *Main.csv and *Details.csv pairs")
    parser.add_argument("--max-field-size", type=int, default=7, help="Maximum field size to consider a small field")
    parser.add_argument("--min-fav-fractional", type=float, default=1.0, help="Minimum favorite fractional odds (e.g., 1.0 = EVS) to exclude chalk")
    parser.add_argument("--out-prefix", default="results_validation", help="Output file prefix for JSON and CSV")
    parser.add_argument("--enrich-exotics", action="store_true", help="Fetch exotic payouts from Sky Sports results pages")
    parser.add_argument("--enrich-limit", type=int, default=None, help="Limit number of races to enrich (for speed/rate limiting)")
    parser.add_argument("--enrich-sleep", type=float, default=0.5, help="Sleep seconds between enrichment fetches")

    args = parser.parse_args()

    main_files = sorted(glob(os.path.join(args.dir, "*Main.csv")))
    if not main_files:
        print(f"No *Main.csv files found in {args.dir}")
        return

    all_results: List[RaceResult] = []

    for main_path in main_files:
        details_path = main_path.replace("Main.csv", "Details.csv")
        if not os.path.exists(details_path):
            print(f"Missing details file for {main_path}")
            continue
        try:
            pair_results = process_pair(main_path, details_path)
            all_results.extend(pair_results)
        except Exception as e:
            print(f"Failed processing {main_path}: {e}")

    if args.enrich_exotics:
        print("Enriching exotic payouts from Sky Sports...")
        enriched_count = enrich_exotics(all_results, max_to_fetch=args.enrich_limit, sleep_seconds=args.enrich_sleep)
        print(f"Enriched races: {enriched_count}")

    filtered = filter_races(all_results, args.max_field_size, args.min_fav_fractional)

    out_prefix = os.path.abspath(args.out_prefix)
    write_outputs(all_results, filtered, out_prefix)

    print(f"Total races: {len(all_results)} | Matches: {len(filtered)}")


if __name__ == "__main__":
    main()