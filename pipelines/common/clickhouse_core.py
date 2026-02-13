from dataclasses import dataclass
from typing import Optional
import clickhouse_connect


@dataclass(frozen=True)
class CHConfig:
    host: str = "clickhouse"
    port: int = 8123
    username: str = "default"
    password: str = "default"
    database: Optional[str] = None


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
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        client.command(stmt)