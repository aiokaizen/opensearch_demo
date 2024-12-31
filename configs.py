from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    """Configuration class used to assemble all applications settings in a single place."""

    model_config = SettingsConfigDict(env_file=".env")

    OS_DB_URL: str = Field("localhost", description="OpenSearch Database URL")
    OS_DB_PORT: int = Field(9200, description="OpenSearch Database port")
    OS_DB_USER: str = Field("admin", description="OpenSearch Database username")
    OS_DB_PWD: str = Field("admin", description="OpenSearch Database pssword")


app_config = AppConfig()
