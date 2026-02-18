from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd

from pipelines.common.clickhouse_core import get_client
from .economic_core import FredClient


def _as_date(s: str):
    return pd.to_datetime(s, errors="coerce").date() if s else None


def _as_dt_utc(s: str):
    if not s:
        return None
    ts = pd.to_datetime(s, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()


def list_active_series(ch) -> list[str]:
    rows = ch.query(
        """
        SELECT series_id
        FROM economic_data.v_fred_series_universe_current
        WHERE is_active = 1
        ORDER BY priority ASC, series_id ASC
        """
    ).result_rows
    return [r[0] for r in rows]


def insert_meta(ch, series_id: str, meta_payload: Dict[str, Any], batch_id: str) -> int:
    seriess = meta_payload.get("seriess", []) or []
    if not seriess:
        print(f"[skip] meta {series_id}: empty")
        return 0

    m = seriess[0]
    row = (
        series_id,
        m.get("title", "") or "",
        m.get("units", "") or "",
        m.get("units_short", "") or "",
        m.get("frequency", "") or "",
        m.get("frequency_short", "") or "",
        m.get("seasonal_adjustment", "") or "",
        m.get("seasonal_adjustment_short", "") or "",
        _as_date(m.get("observation_start", "")),
        _as_date(m.get("observation_end", "")),
        _as_dt_utc(m.get("last_updated", "")),
        int(m.get("popularity") or 0),
        m.get("notes", "") or "",
        "fred_api",
        datetime.now(timezone.utc),
        batch_id,
    )

    ch.insert(
        "economic_data.fred_series_meta",
        [row],
        column_names=[
            "series_id",
            "title",
            "units",
            "units_short",
            "frequency",
            "frequency_short",
            "seasonal_adjustment",
            "seasonal_adjustment_short",
            "observation_start",
            "observation_end",
            "last_updated",
            "popularity",
            "notes",
            "source",
            "built_at",
            "batch_id",
        ],
    )
    return 1


def main():
    ch = get_client()
    fred = FredClient.from_env()

    series_ids = list_active_series(ch)
    if not series_ids:
        print("[warn] no active series in economic_data.fred_series_universe")
        return

    batch_id = f"fred_meta_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    ok = 0

    for sid in series_ids:
        try:
            meta = fred.series(sid)
            ok += insert_meta(ch, sid, meta, batch_id=batch_id)
        except Exception as e:
            print(f"[err] meta {sid}: {e}")

    print(f"[ok] discovery complete: inserted_meta={ok} batch_id={batch_id}")


if __name__ == "__main__":
    main()