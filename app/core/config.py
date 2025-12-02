from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    APP_ENV: str = "prod"
    APP_NAME: str = "Plataforma_SurChile"
    API_V1_PREFIX: str = "/api/v1"
    MONGO_URI: str
    MONGO_DB: str
    WMS_URL: str = ""
    WMS_USER: str = ""
    WMS_PASS: str = ""
    WMS_DEFAULT_WAREHOUSE: str = ""
    WMS_TIMEOUT_SECONDS: int = 30
    WMS_QUERY_URL_QA: str = ""
    WMS_QUERY_URL_PROD: str = ""
    WMS_LOGIN_URL_QA: str = ""
    WMS_LOGIN_URL_PROD: str = ""
    LOG_LEVEL: str = "INFO"

    # indica dónde leer .env en local
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra='ignore'
    )

settings = Settings()
