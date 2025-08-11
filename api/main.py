from fastapi import FastAPI
from typing import List, Dict, Any

app = FastAPI(title="UltraUtopia API", version="0.1.0")

# Stubs – wire to SQLite queries in a follow-up

@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}

@app.get("/races")
def list_races(limit: int = 50) -> List[Dict[str, Any]]:
    # TODO: query SQLite, order upcoming
    return []

@app.get("/events")
def list_events(kind: str = "steamer", limit: int = 50) -> List[Dict[str, Any]]:
    # TODO: query SQLite by kind
    return []