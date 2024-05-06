from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    BOT_TOKEN: str
    MONGODB_URL: str

    model_config = SettingsConfigDict(
        env_file="../.env.docker", env_file_encoding="utf-8"
    )


settings = Settings()
