from .core import ingest_symbol
from .symbols.all import ALL_SYMBOLS
import clickhouse_connect

def main():
    client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="default",
        password="default",
    )

    for s in ALL_SYMBOLS:
        ingest_symbol(client, s, mode="backfill")

if __name__ == "__main__":
    main()