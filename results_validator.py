#!/usr/bin/env python3

import argparse
import csv
import json
import math
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from glob import glob
from typing import List, Optional, Dict, Any


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
            # Normalize keys and strip values
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
    # Expected like 10-Mar-25
    try:
        return datetime.strptime(date_str.strip(), "%d-%b-%y")
    except Exception:
        return None


def process_pair(main_path: str, details_path: str) -> List[RaceResult]:
    main_rows = parse_main_csv(main_path)
    details_rows = parse_details_csv(details_path)

    # Index details by MainID (-> main Id)
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

        # Identify winner
        winner: Optional[RunnerResult] = None
        for r in runners:
            if (r.finishing_pos or "").strip() in {"1", "1st"}:
                winner = r
                break

        # Favorite and second favorite by Favs label
        favorite = next((r for r in runners if r.fav_label and r.fav_label.lower().startswith("fav")), None)
        second_favorite = next((r for r in runners if r.fav_label and r.fav_label.lower().startswith("2fav")), None)

        fav_frac = favorite.sp_fractional if favorite else None
        sec_frac = second_favorite.sp_fractional if second_favorite else None
        odds_ratio = None
        if fav_frac is not None and fav_frac > 0 and sec_frac is not None:
            odds_ratio = sec_frac / fav_frac

        winner_return = None
        if winner and winner.sp_fractional is not None:
            # UK fractional payout on 1 unit stake = profit + stake = (f + 1)
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


def filter_races(races: List[RaceResult], max_field_size: int, min_fav_fractional: float) -> List[RaceResult]:
    filtered: List[RaceResult] = []
    for r in races:
        if r.field_size <= max_field_size:
            # If we cannot determine favorite odds, keep it (conservative) or skip?
            # Keep it to avoid false negatives.
            if r.fav_fractional is None or r.fav_fractional >= min_fav_fractional:
                filtered.append(r)
    return filtered


def write_outputs(races_all: List[RaceResult], races_filtered: List[RaceResult], out_prefix: str) -> None:
    # JSON
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

    # CSV (matches only)
    csv_path = f"{out_prefix}.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "source_file", "date", "time", "course", "race_desc", "field_size",
            "favorite", "fav_fractional", "second_favorite", "second_fav_fractional",
            "odds_ratio_second_over_fav", "winner", "winner_fractional", "winner_return_on_1",
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
            ])

    print(f"Wrote: {json_path} and {csv_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate tipsheet against historical results (small fields, no chalk favorites)")
    parser.add_argument("--dir", default="/workspace/old_results", help="Directory containing *Main.csv and *Details.csv pairs")
    parser.add_argument("--max-field-size", type=int, default=7, help="Maximum field size to consider a small field")
    parser.add_argument("--min-fav-fractional", type=float, default=1.0, help="Minimum favorite fractional odds (e.g., 1.0 = EVS) to exclude chalk")
    parser.add_argument("--out-prefix", default="results_validation", help="Output file prefix for JSON and CSV")

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

    filtered = filter_races(all_results, args.max_field_size, args.min_fav_fractional)

    out_prefix = os.path.abspath(args.out_prefix)
    write_outputs(all_results, filtered, out_prefix)

    print(f"Total races: {len(all_results)} | Matches: {len(filtered)}")


if __name__ == "__main__":
    main()