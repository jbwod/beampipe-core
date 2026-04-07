import os
from enum import StrEnum
from typing import Literal

from pydantic import SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LoggingSettings(BaseSettings):
    LOG_VERBOSITY: Literal["full", "medium", "minimal"] = "full"


class AppSettings(BaseSettings):
    APP_NAME: str = "beampipe-core"
    APP_DESCRIPTION: str | None = (
        "Event-driven control plane for scheduler-aware radio astronomy workflows."
    )
    APP_VERSION: str | None = None
    LICENSE_NAME: str | None = "MIT"
    CONTACT_NAME: str | None = None
    CONTACT_EMAIL: str | None = None


class CryptSettings(BaseSettings):
    SECRET_KEY: SecretStr = SecretStr("secret-key")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7


class DatabaseSettings(BaseSettings):
    pass


class SQLiteSettings(DatabaseSettings):
    SQLITE_URI: str = "./sql_app.db"
    SQLITE_SYNC_PREFIX: str = "sqlite:///"
    SQLITE_ASYNC_PREFIX: str = "sqlite+aiosqlite:///"


class MySQLSettings(DatabaseSettings):
    MYSQL_USER: str = "username"
    MYSQL_PASSWORD: str = "password"
    MYSQL_SERVER: str = "localhost"
    MYSQL_PORT: int = 5432
    MYSQL_DB: str = "dbname"
    MYSQL_SYNC_PREFIX: str = "mysql://"
    MYSQL_ASYNC_PREFIX: str = "mysql+aiomysql://"
    MYSQL_URL: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def MYSQL_URI(self) -> str:
        credentials = f"{self.MYSQL_USER}:{self.MYSQL_PASSWORD}"
        location = f"{self.MYSQL_SERVER}:{self.MYSQL_PORT}/{self.MYSQL_DB}"
        return f"{credentials}@{location}"


class PostgresSettings(DatabaseSettings):
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "postgres"
    POSTGRES_SYNC_PREFIX: str = "postgresql://"
    POSTGRES_ASYNC_PREFIX: str = "postgresql+asyncpg://"
    POSTGRES_URL: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def POSTGRES_URI(self) -> str:
        credentials = f"{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
        location = f"{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        return f"{credentials}@{location}"


class FirstUserSettings(BaseSettings):
    ADMIN_NAME: str = "admin"
    ADMIN_EMAIL: str = "admin@admin.com"
    ADMIN_USERNAME: str = "admin"
    ADMIN_PASSWORD: str = "!Ch4ng3Th1sP4ssW0rd!"


class TestSettings(BaseSettings):
    ...


class RedisCacheSettings(BaseSettings):
    REDIS_CACHE_HOST: str = "localhost"
    REDIS_CACHE_PORT: int = 6379

    @computed_field  # type: ignore[prop-decorator]
    @property
    def REDIS_CACHE_URL(self) -> str:
        return f"redis://{self.REDIS_CACHE_HOST}:{self.REDIS_CACHE_PORT}"


class ClientSideCacheSettings(BaseSettings):
    CLIENT_CACHE_MAX_AGE: int = 60


class RedisQueueSettings(BaseSettings):
    REDIS_QUEUE_HOST: str = "localhost"
    REDIS_QUEUE_PORT: int = 6379
    WORKER_QUEUE_NAME: str = "arq:queue"
    SCHEDULER_QUEUE_NAME: str = "arq:scheduler"


class RedisRateLimiterSettings(BaseSettings):
    REDIS_RATE_LIMIT_HOST: str = "localhost"
    REDIS_RATE_LIMIT_PORT: int = 6379

    @computed_field  # type: ignore[prop-decorator]
    @property
    def REDIS_RATE_LIMIT_URL(self) -> str:
        return f"redis://{self.REDIS_RATE_LIMIT_HOST}:{self.REDIS_RATE_LIMIT_PORT}"


class DefaultRateLimitSettings(BaseSettings):
    DEFAULT_RATE_LIMIT_LIMIT: int = 10
    DEFAULT_RATE_LIMIT_PERIOD: int = 3600


class CRUDAdminSettings(BaseSettings):
    CRUD_ADMIN_ENABLED: bool = True
    CRUD_ADMIN_MOUNT_PATH: str = "/admin"

    CRUD_ADMIN_ALLOWED_IPS_LIST: list[str] | None = None
    CRUD_ADMIN_ALLOWED_NETWORKS_LIST: list[str] | None = None
    CRUD_ADMIN_MAX_SESSIONS: int = 10
    CRUD_ADMIN_SESSION_TIMEOUT: int = 1440
    SESSION_SECURE_COOKIES: bool = True

    CRUD_ADMIN_TRACK_EVENTS: bool = True
    CRUD_ADMIN_TRACK_SESSIONS: bool = True

    CRUD_ADMIN_REDIS_ENABLED: bool = False
    CRUD_ADMIN_REDIS_HOST: str = "localhost"
    CRUD_ADMIN_REDIS_PORT: int = 6379
    CRUD_ADMIN_REDIS_DB: int = 0
    CRUD_ADMIN_REDIS_PASSWORD: str | None = "None"
    CRUD_ADMIN_REDIS_SSL: bool = False


class EnvironmentOption(StrEnum):
    LOCAL = "local"
    STAGING = "staging"
    PRODUCTION = "production"


class EnvironmentSettings(BaseSettings):
    ENVIRONMENT: EnvironmentOption = EnvironmentOption.LOCAL


class CORSSettings(BaseSettings):
    CORS_ORIGINS: list[str] = ["*"]
    CORS_METHODS: list[str] = ["*"]
    CORS_HEADERS: list[str] = ["*"]


class ExecutionLedgerSettings(BaseSettings):
    MAX_RETRIES: int = 3
    WORKFLOW_AUTOMATION_SCHEDULER_NAME: str = "workflow_auto"
    WORKFLOW_EXECUTION_AUTOMATION_ENABLED: bool = True


class RestateWorkflowSettings(BaseSettings):
    WORKFLOW_ENGINE_EXECUTION: Literal["arq", "restate"] = "arq"
    WORKFLOW_ENGINE_DISCOVERY: Literal["arq", "restate"] = "arq"

    RESTATE_INGRESS_BASE_URL: str = ""

    RESTATE_EXECUTION_WORKFLOW_NAME: str = "ExecuteExecutionWorkflow"
    RESTATE_DISCOVERY_WORKFLOW_NAME: str = "DiscoveryBatchWorkflow"
    RESTATE_EXECUTION_WORKFLOW_HANDLER: str = "execute_execution_workflow"
    RESTATE_DISCOVERY_WORKFLOW_HANDLER: str = "discovery_batch_workflow"

    RESTATE_INVOKE_TIMEOUT_SECONDS: float = 30.0
    # ctx.run_typed policies (https://docs.restate.dev/develop/python/durable-steps).
    RESTATE_STEP_EXTERNAL_MAX_ATTEMPTS: int = 12
    RESTATE_STEP_EXTERNAL_MAX_DURATION_MINUTES: int = 45
    RESTATE_STEP_DB_MAX_ATTEMPTS: int = 10
    RESTATE_STEP_DB_MAX_DURATION_MINUTES: int = 15
    RESTATE_STEP_POLL_MAX_ATTEMPTS: int = 6
    RESTATE_STEP_POLL_MAX_DURATION_MINUTES: int = 5
    RESTATE_STEP_INITIAL_RETRY_SECONDS: float = 2.0
    RESTATE_STEP_MAX_RETRY_INTERVAL_SECONDS: float = 120.0


class DiscoverySettings(BaseSettings):
    DISCOVERY_BATCH_SIZE: int = 50
    DISCOVERY_BATCH_CONCURRENCY: int = 5
    DISCOVERY_STALE_HOURS: int = 24
    DISCOVERY_SCHEDULE_MINUTES: int = 1
    DISCOVERY_TAP_TIMEOUT_SECONDS: int = 120
    DISCOVERY_RETRY_COOLDOWN_MINUTES: int = 60
    DISCOVERY_CLAIM_TTL_MINUTES: int = 180
    DISCOVERY_MAX_FAILURES_BEFORE_BACKOFF_MULTIPLIER: int = 3
    DISCOVERY_MAX_SOURCES_PER_RUN: int = 2000
    DISCOVERY_TAP_HEALTH_CHECK_ENABLED: bool = True
    DISCOVERY_TAP_HEALTH_TIMEOUT_SECONDS: float = 10.0


class ShapingSettings(BaseSettings):
    # Hybrid Shaper
    SHAPING_ARQ_QUEUE_MAX_DEPTH: int | None = 200

    # Discovery admission (token bucket)
    SHAPING_DISCOVERY_RATE_PER_MIN: int = 200
    SHAPING_DISCOVERY_BURST: int = 400
    # Discovery in-flight guard
    SHAPING_DISCOVERY_MAX_IN_FLIGHT_BATCHES: int | None = 4

    # Execute admission (token bucket)
    SHAPING_EXECUTE_RATE_PER_MIN: int = 20
    SHAPING_EXECUTE_BURST: int = 40
    # Execute in-flight guard
    SHAPING_EXECUTE_MAX_IN_FLIGHT_RUNS: int | None = 2

    # Sleep after each successful enqueue_job
    SHAPING_ENQUEUE_PACING_MS: float = 0.0

    # Cap discover_batch
    SHAPING_DISCOVERY_MAX_JOBS_PER_TICK: int | None = None


class ArchiveSettings(BaseSettings):
    ARCHIVE_METADATA_VALIDATE_JSON: bool = False


class CasdaSettings(BaseSettings):
    CASDA_USERNAME: str | None = None
    CASDA_PASSWORD: SecretStr | None = None


class Settings(
    LoggingSettings,
    AppSettings,
    SQLiteSettings,
    PostgresSettings,
    CryptSettings,
    FirstUserSettings,
    TestSettings,
    RedisCacheSettings,
    ClientSideCacheSettings,
    RedisQueueSettings,
    RedisRateLimiterSettings,
    DefaultRateLimitSettings,
    CRUDAdminSettings,
    EnvironmentSettings,
    CORSSettings,
    ExecutionLedgerSettings,
    RestateWorkflowSettings,
    DiscoverySettings,
    ShapingSettings,
    ArchiveSettings,
    CasdaSettings,
):
    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", ".env"),
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


settings = Settings()
