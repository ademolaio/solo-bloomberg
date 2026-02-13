from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, Optional, Tuple

import clickhouse_connect


@dataclass(frozen=True)
class CHConfig:
    host: str = "clickhouse"
    port: int = 8123
    username: str = "default"
    password: str = "default"
    database: Optional[str] = None  # or "default"


def get_client(cfg: CHConfig | None = None):
    cfg = cfg or CHConfig()
    return clickhouse_connect.get_client(
        host=cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
    )


def exec_sql(client, sql: str):
    # supports multi-statement strings separated by ;
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        client.command(stmt)


def year_ranges(min_year: int, max_year: int) -> Iterable[Tuple[date, date]]:
    # [start, end) windows
    for y in range(min_year, max_year + 1):
        yield date(y, 1, 1), date(y + 1, 1, 1)


def month_ranges(min_year: int, max_year: int) -> Iterable[Tuple[date, date]]:
    # [start, end) windows per month
    for y in range(min_year, max_year + 1):
        for m in range(1, 13):
            start = date(y, m, 1)
            if m == 12:
                end = date(y + 1, 1, 1)
            else:
                end = date(y, m + 1, 1)
            yield start, end


def backfill_by_ranges(
    client,
    insert_sql_template: str,
    ranges: Iterable[Tuple[date, date]],
    label: str,
):
    """
    insert_sql_template MUST include:
      {start} and {end} placeholders (YYYY-MM-DD)
    """
    for start, end in ranges:
        sql = insert_sql_template.format(start=start.isoformat(), end=end.isoformat())
        print(f"[backfill:{label}] {start} -> {end}")
        client.command(sql)