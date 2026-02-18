from __future__ import annotations

from collections import defaultdict

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from pipelines.common.clickhouse_core import get_client
from .economic_core import FredClient


def _as_date(s: str):
    return pd.to_datetime(s, errors="coerce").date() if s else None

def _chunk_rows_by_yyyymm(rows: list[tuple]) -> list[list[tuple]]:
    """
    rows tuples are:
      (series_id, date, value, is_missing, realtime_start, realtime_end, source, ingested_at, batch_id)
                 ^ index 1 is the 'date' column (datetime.date)
    """
    buckets: dict[int, list[tuple]] = defaultdict(list)
    for r in rows:
        d = r[1]
        buckets[d.year * 100 + d.month].append(r)

    return [buckets[k] for k in sorted(buckets)]


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


def insert_observations(
    ch,
    series_id: str,
    obs: List[Dict[str, Any]],
    batch_id: str,
) -> int:
    if not obs:
        return 0

    now = datetime.now(timezone.utc)
    rows = []

    for o in obs:
        d = _as_date(o.get("date", ""))
        if d is None:
            continue

        v_raw = o.get("value", None)
        if v_raw in (None, ".", ""):
            # default policy: skip missing points entirely
            continue

        v = pd.to_numeric(v_raw, errors="coerce")
        if pd.isna(v):
            continue

        rs = _as_date(o.get("realtime_start", "")) or d
        re = _as_date(o.get("realtime_end", "")) or d

        rows.append(
            (
                series_id,
                d,
                float(v),
                0,
                rs,
                re,
                "fred_api",
                now,
                batch_id,
            )
        )

    if not rows:
        return 0

    inserted = 0
    for chunk in _chunk_rows_by_yyyymm(rows):
        ch.insert(
            "economic_data.fred_observations",
            chunk,
            column_names=[
                "series_id",
                "date",
                "value",
                "is_missing",
                "realtime_start",
                "realtime_end",
                "source",
                "ingested_at",
                "batch_id",
            ],
        )
        inserted += len(chunk)

    return inserted


def ingest_one_series(
    fred: FredClient,
    ch,
    *,
    series_id: str,
    observation_start: str = "1970-01-01",
    observation_end: Optional[str] = None,
    page_limit: int = 100000,
) -> int:
    batch_id = f"fred_obs_{series_id}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    total_inserted = 0
    offset = 0

    while True:
        payload = fred.series_observations(
            series_id,
            observation_start=observation_start,
            observation_end=observation_end,
            limit=page_limit,
            offset=offset,
            sort_order="asc",
        )

        obs = payload.get("observations", []) or []
        total_inserted += insert_observations(ch, series_id, obs, batch_id=batch_id)

        count = int(payload.get("count") or 0)
        limit = int(payload.get("limit") or page_limit)
        got = len(obs)

        if got == 0:
            break

        offset += limit
        if offset >= count:
            break

    print(f"[ok] {series_id}: inserted_obs={total_inserted} batch_id={batch_id}")
    return total_inserted


def main(observation_start: str = "1970-01-01"):
    ch = get_client()
    fred = FredClient.from_env()

    series_ids = list_active_series(ch)
    if not series_ids:
        print("[warn] no active series in economic_data.fred_series_universe")
        return

    total = 0
    for sid in series_ids:
        try:
            total += ingest_one_series(fred, ch, series_id=sid, observation_start=observation_start)
        except Exception as e:
            print(f"[err] obs {sid}: {e}")

    print(f"[ok] observations ingestion complete: total_inserted={total}")


if __name__ == "__main__":
    main()