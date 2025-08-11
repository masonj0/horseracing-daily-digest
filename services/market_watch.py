import asyncio
import aiosqlite
from datetime import datetime, timezone
from typing import List, Dict

from db import add_snapshot, add_event, get_last_snapshot, commit

STEAMER_DROP = 0.5  # fractional odds drop threshold
DRIFTER_RISE = 0.5


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_fractional(odds_str: str) -> float:
    if not odds_str:
        return None
    s = odds_str.strip().upper().replace('-', '/').replace(' ', '')
    if s in ('SP', 'NR', ''):
        return None
    if s in ('EVS', 'EVENS'):
        return 1.0
    try:
        if '/' in s:
            a,b = s.split('/',1)
            a=float(a); b=float(b)
            if b<=0: return None
            return a/b
        d = float(s)
        return d-1.0 if d>1 else None
    except Exception:
        return None


async def snapshot_race(db: aiosqlite.Connection, race: Dict):
    race_id = race['id']
    ts = _now_iso()
    for rr in race.get('all_runners', []):
        name = rr.get('name')
        odds = _to_fractional(rr.get('odds_str'))
        await add_snapshot(db, race_id, name, odds, ts)
        last = await get_last_snapshot(db, race_id, name)
        if last is not None and odds is not None:
            if (last - odds) >= STEAMER_DROP:
                await add_event(db, race_id, name, 'steamer', last, odds, ts)
            elif (odds - last) >= DRIFTER_RISE:
                await add_event(db, race_id, name, 'drifter', last, odds, ts)


async def watch_loop(db_path: str, fetch_upcoming_races, interval_sec: int = 75):
    async with aiosqlite.connect(db_path) as db:
        while True:
            races: List[Dict] = await fetch_upcoming_races()
            for r in races:
                try:
                    await snapshot_race(db, r)
                except Exception:
                    continue
            await commit(db)
            await asyncio.sleep(interval_sec)