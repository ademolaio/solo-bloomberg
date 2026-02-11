from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    CLICKHOUSE_HOST: str = "clickhouse"
    CLICKHOUSE_HTTP_PORT: int = 8123
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = "default"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()