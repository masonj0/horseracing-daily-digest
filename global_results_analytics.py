#!/usr/bin/env python3

import argparse
import json
import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

# Matplotlib (headless)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Reuse ATR scraping helpers from exotics_scourer if available
try:
    from exotics_scourer import (
        atr_list_meetings_for_date,
        atr_list_race_result_links,
        atr_parse_race_result,
    )
    HAS_ATR = True
except Exception:
    HAS_ATR = False


@dataclass
class NormalizedRace:
    source: str
    date: str
    course: str
    time: str
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
    url: Optional[str] = None
    winner_return_on_1: Optional[float] = None


# ---------------- Ingestors ----------------

def ingest_from_validator_json(json_path: str) -> List[NormalizedRace]:
    if not os.path.exists(json_path):
        return []
    data = json.load(open(json_path, "r", encoding="utf-8"))
    races = []
    for r in data.get("all", []):
        races.append(NormalizedRace(
            source="validator_zip",
            date=r.get("date_str", ""),
            course=r.get("course", ""),
            time=r.get("time", ""),
            field_size=int(r.get("field_size", 0) or 0),
            favorite_fractional=r.get("fav_fractional"),
            second_favorite_fractional=r.get("second_fav_fractional"),
            favorite_name=(r.get("favorite") or {}).get("name") if isinstance(r.get("favorite"), dict) else None,
            second_favorite_name=(r.get("second_favorite") or {}).get("name") if isinstance(r.get("second_favorite"), dict) else None,
            csf=r.get("csf"),
            tricast=r.get("tricast"),
            exacta=r.get("exacta"),
            trifecta=r.get("trifecta"),
            superfecta=r.get("superfecta"),
            url=None,
            winner_return_on_1=r.get("winner_return_on_1"),
        ))
    return races


def ingest_from_atr(days_back: int, meeting_limit: Optional[int], race_limit: Optional[int], sleep_seconds: float) -> List[NormalizedRace]:
    if not HAS_ATR:
        return []
    all_races: List[NormalizedRace] = []
    today = datetime.utcnow().date()
    count = 0
    for i in range(days_back):
        d = today - timedelta(days=i)
        meetings = atr_list_meetings_for_date(datetime(d.year, d.month, d.day))
        if meeting_limit is not None:
            meetings = meetings[:meeting_limit]
        for m_url in meetings:
            links = atr_list_race_result_links(m_url)
            for r_url in links:
                if race_limit is not None and count >= race_limit:
                    return all_races
                try:
                    er = atr_parse_race_result(r_url)
                except Exception:
                    er = None
                if er:
                    all_races.append(NormalizedRace(
                        source="atr",
                        date=er.date,
                        course=er.course,
                        time=er.time,
                        field_size=int(er.field_size or 0),
                        favorite_fractional=er.favorite_fractional,
                        second_favorite_fractional=er.second_favorite_fractional,
                        favorite_name=er.favorite_name,
                        second_favorite_name=er.second_favorite_name,
                        csf=er.csf,
                        tricast=er.tricast,
                        exacta=er.exacta,
                        trifecta=er.trifecta,
                        superfecta=er.superfecta,
                        url=er.url,
                        winner_return_on_1=(er.favorite_fractional + 1.0) if isinstance(er.favorite_fractional, (int, float)) else None,
                    ))
                    count += 1
                if sleep_seconds:
                    time.sleep(sleep_seconds)
    return all_races


# ---------------- Analytics / Plots ----------------

def parse_money(s: Optional[str]) -> Optional[float]:
    if not isinstance(s, str) or not s.strip():
        return None
    t = re.sub(r"[^0-9.,]", "", s).replace(",", "")
    try:
        return float(t)
    except Exception:
        return None


def build_plots(races: List[NormalizedRace], out_dir: str) -> List[str]:
    os.makedirs(out_dir, exist_ok=True)
    written: List[str] = []

    # Winner-return hist
    wr_all = [r.winner_return_on_1 for r in races if isinstance(r.winner_return_on_1, (int, float)) and r.winner_return_on_1 > 0]
    if wr_all:
        plt.figure(figsize=(9,5))
        plt.hist(wr_all, bins=30, alpha=0.7, color="#4e79a7")
        plt.xlabel("Winner return on 1 unit")
        plt.ylabel("Count")
        plt.title("Winner Return Distribution (Global Sources)")
        plt.grid(alpha=0.2)
        plt.tight_layout()
        p = os.path.join(out_dir, "global_winner_return_hist.png")
        plt.savefig(p, dpi=140)
        written.append(p)

    # Exotics hists
    for key, label in [("csf","CSF"),("tricast","Tricast"),("exacta","Exacta"),("trifecta","Trifecta"),("superfecta","Superfecta/First4")]:
        vals = [parse_money(getattr(r, key)) for r in races]
        vals = [v for v in vals if isinstance(v, float) and v > 0]
        if not vals:
            continue
        plt.figure(figsize=(9,5))
        plt.hist(vals, bins=30, alpha=0.75, color="#e15759")
        plt.xlabel(f"{label} payout")
        plt.ylabel("Count")
        plt.title(f"{label} Payouts (Global Sources)")
        plt.grid(alpha=0.2)
        plt.tight_layout()
        p = os.path.join(out_dir, f"global_{key}_hist.png")
        plt.savefig(p, dpi=140)
        written.append(p)

    # Fav vs winner return scatter
    x=[]; y=[]
    for r in races:
        fav = r.favorite_fractional
        win = r.winner_return_on_1
        if isinstance(fav,(int,float)) and isinstance(win,(int,float)):
            x.append(float(fav)); y.append(float(win))
    if x:
        plt.figure(figsize=(9,5))
        plt.scatter(x, y, s=16, alpha=0.6, color="#59a14f")
        plt.xlabel("Favorite fractional (profit per 1)")
        plt.ylabel("Winner return on 1")
        plt.title("Winner Return vs Favorite Odds (Global)")
        plt.grid(alpha=0.2)
        plt.tight_layout()
        p = os.path.join(out_dir, "global_return_vs_fav.png")
        plt.savefig(p, dpi=140)
        written.append(p)

    return written


# ---------------- CLI ----------------

def main():
    ap = argparse.ArgumentParser(description="Global results analytics with exotics and plots")
    ap.add_argument("--days-back", type=int, default=2, help="Days back to scan for ATR results")
    ap.add_argument("--meeting-limit", type=int, default=8, help="Limit meetings per day (ATR)")
    ap.add_argument("--race-limit", type=int, default=200, help="Global race limit (ATR)")
    ap.add_argument("--sleep", type=float, default=0.25, help="Sleep between ATR requests")
    ap.add_argument("--zip-json", default="/workspace/old_results/validation_eng_ire_enriched.json", help="Existing validator JSON to include")
    ap.add_argument("--out-dir", default="/workspace/global_results_output", help="Directory to write plots")
    args = ap.parse_args()

    races: List[NormalizedRace] = []

    # Include historical ZIP-derived dataset (if present)
    if os.path.exists(args.zip_json):
        races.extend(ingest_from_validator_json(args.zip_json))

    # Include ATR recent results
    races.extend(ingest_from_atr(args.days_back, args.meeting_limit, args.race_limit, args.sleep))

    written = build_plots(races, args.out_dir)
    if written:
        print("Wrote:")
        for p in written:
            print(" ", p)
    else:
        print("No plots generated (no data found). Try increasing days-back or ensure JSON path exists.")


if __name__ == "__main__":
    main()