import uuid
from pathlib import Path

from pipeline.regional_securities.all_securities import ALL_SECURITIES
from pipeline.extract.market_data.yfinance_prices import fetch_daily_history_max
from pipeline.transform.market_data.clean_prices import clean_daily_bars
from pipeline.load.market_data.daily_prices import get_ch_client, insert_daily_prices

def ensure_schema():
    sql = Path("pipeline/sql/market_data/daily_prices.sql").read_text()
    client = get_ch_client()
    client.command(sql)

def main():
    ensure_schema()
    batch_id = str(uuid.uuid4())

    total = 0
    for symbol in ALL_SECURITIES:
        short_name, df = fetch_daily_history_max(symbol)
        df = clean_daily_bars(df)
        total += insert_daily_prices(
            symbol=symbol,
            short_name=short_name,
            bars=df,
            batch_id=batch_id,
            source="yfinance",
        )
    print(f"Inserted rows: {total}")

if __name__ == "__main__":
    main()