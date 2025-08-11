import aiosqlite
from typing import List, Optional, Dict, Any

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS races (
      id TEXT PRIMARY KEY,
      course TEXT,
      country TEXT,
      discipline TEXT,
      utc_datetime TEXT,
      local_time TEXT,
      timezone_name TEXT,
      field_size INTEGER,
      value_score REAL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS runners (
      race_id TEXT,
      name TEXT,
      upi_score REAL,
      features_json TEXT,
      odds_str TEXT,
      PRIMARY KEY(race_id, name)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS odds_snapshots (
      race_id TEXT,
      runner_name TEXT,
      odds REAL,
      ts TEXT,
      PRIMARY KEY(race_id, runner_name, ts)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS market_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      race_id TEXT,
      runner_name TEXT,
      kind TEXT,
      from_odds REAL,
      to_odds REAL,
      ts TEXT
    );
    """,
]


async def init_db(path: str):
    async with aiosqlite.connect(path) as db:
        for stmt in SCHEMA:
            await db.execute(stmt)
        await db.commit()


async def upsert_race(db: aiosqlite.Connection, race: Dict[str, Any]):
    await db.execute(
        """
        INSERT INTO races(id, course, country, discipline, utc_datetime, local_time, timezone_name, field_size, value_score)
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
          course=excluded.course,
          country=excluded.country,
          discipline=excluded.discipline,
          utc_datetime=excluded.utc_datetime,
          local_time=excluded.local_time,
          timezone_name=excluded.timezone_name,
          field_size=excluded.field_size,
          value_score=excluded.value_score
        """,
        (
            race["id"], race.get("course"), race.get("country"), race.get("discipline"),
            race.get("utc_datetime"), race.get("local_time"), race.get("timezone_name"),
            race.get("field_size"), race.get("value_score")
        )
    )


async def upsert_runner(db: aiosqlite.Connection, runner: Dict[str, Any]):
    await db.execute(
        """
        INSERT INTO runners(race_id, name, upi_score, features_json, odds_str)
        VALUES(?,?,?,?,?)
        ON CONFLICT(race_id, name) DO UPDATE SET
          upi_score=excluded.upi_score,
          features_json=excluded.features_json,
          odds_str=excluded.odds_str
        """,
        (
            runner["race_id"], runner["name"], runner.get("upi_score"), runner.get("features_json"), runner.get("odds_str")
        )
    )


async def add_snapshot(db: aiosqlite.Connection, race_id: str, runner_name: str, odds: Optional[float], ts: str):
    await db.execute(
        """
        INSERT OR IGNORE INTO odds_snapshots(race_id, runner_name, odds, ts) VALUES(?,?,?,?)
        """,
        (race_id, runner_name, odds, ts)
    )


async def add_event(db: aiosqlite.Connection, race_id: str, runner_name: str, kind: str, from_odds: Optional[float], to_odds: Optional[float], ts: str):
    await db.execute(
        """
        INSERT INTO market_events(race_id, runner_name, kind, from_odds, to_odds, ts) VALUES(?,?,?,?,?,?)
        """,
        (race_id, runner_name, kind, from_odds, to_odds, ts)
    )


async def get_last_snapshot(db: aiosqlite.Connection, race_id: str, runner_name: str) -> Optional[float]:
    async with db.execute(
        "SELECT odds FROM odds_snapshots WHERE race_id=? AND runner_name=? ORDER BY ts DESC LIMIT 1",
        (race_id, runner_name)
    ) as cur:
        row = await cur.fetchone()
        return row[0] if row else None


async def commit(db: aiosqlite.Connection):
    await db.commit()