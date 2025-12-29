# ==============================================================================
# ABInBev Case - Settings/Configuration Module
# ==============================================================================
#
# Este modulo centraliza todas as configuracoes do projeto.
# Funciona tanto em ambiente local quanto em cloud.
#
# ==============================================================================

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from config.secrets import get_secret

# Carrega variaveis de ambiente do arquivo .env
load_dotenv()


@dataclass
class StorageConfig:
    """Configuracoes de armazenamento."""

    landing_path: str
    bronze_path: str
    silver_path: str
    gold_path: str
    consumption_path: str
    control_path: str


@dataclass
class SparkConfig:
    """Configuracoes do Spark."""

    app_name: str
    driver_memory: str
    executor_memory: str
    shuffle_partitions: int


@dataclass
class Settings:
    """Configuracoes centralizadas do projeto."""

    environment: str
    storage: StorageConfig
    spark: SparkConfig
    source_path: str

    @property
    def is_local(self) -> bool:
        return self.environment == "local"

    @property
    def is_cloud(self) -> bool:
        return self.environment in ["dev", "staging", "prod"]


def get_settings() -> Settings:
    """
    Retorna as configuracoes baseadas no ambiente.

    Ambiente e definido pela variavel ENVIRONMENT:
    - local: Usa paths locais
    - dev/staging/prod: Usa Azure Storage

    Returns:
        Settings: Objeto com todas as configuracoes
    """
    environment = os.getenv("ENVIRONMENT", "local")

    if environment == "local":
        # Paths locais
        base_path = Path(os.getenv("LOCAL_BASE_PATH", "./data"))
        storage = StorageConfig(
            landing_path=str(base_path / "landing"),
            bronze_path=str(base_path / "bronze"),
            silver_path=str(base_path / "silver"),
            gold_path=str(base_path / "gold"),
            consumption_path=str(base_path / "consumption"),
            control_path=str(base_path / "control"),
        )
        source_path = os.getenv(
            "LOCAL_SOURCE_PATH", "./ABI DENG Recrutiment Business Case 1 20210727"
        )
    else:
        # Azure Storage paths
        account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "abinbevdatalake")
        base = f"abfss://{{container}}@{account}.dfs.core.windows.net"

        storage = StorageConfig(
            landing_path=base.format(container="landing"),
            bronze_path=base.format(container="bronze"),
            silver_path=base.format(container="silver"),
            gold_path=base.format(container="gold"),
            consumption_path=base.format(container="consumption"),
            control_path=base.format(container="control"),
        )
        source_path = f"{storage.landing_path}/source"

    spark = SparkConfig(
        app_name=os.getenv("SPARK_APP_NAME", "ABInBev_Beverage_Analytics"),
        driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "4g"),
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
        shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")),
    )

    return Settings(
        environment=environment,
        storage=storage,
        spark=spark,
        source_path=source_path,
    )


def get_spark_session(settings: Optional[Settings] = None):
    """
    Cria ou obtem SparkSession configurada para o ambiente.

    Em ambiente cloud (Databricks/HDInsight), o spark ja esta disponivel.
    Em ambiente local, cria uma nova sessao.

    Args:
        settings: Configuracoes (opcional, usa get_settings() se None)

    Returns:
        SparkSession configurada
    """
    from pyspark.sql import SparkSession

    if settings is None:
        settings = get_settings()

    # Tenta usar spark existente (Databricks/HDInsight)
    try:
        existing = SparkSession.getActiveSession()
        if existing:
            print("[INFO] Usando SparkSession existente")
            return existing
    except RuntimeError:
        # SparkSession.getActiveSession() pode falhar se nao houver contexto Spark
        pass

    # Cria nova sessao (ambiente local)
    print(f"[INFO] Criando nova SparkSession para ambiente: {settings.environment}")

    builder = (
        SparkSession.builder.appName(settings.spark.app_name)
        .config("spark.driver.memory", settings.spark.driver_memory)
        .config("spark.sql.shuffle.partitions", settings.spark.shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "true")
    )

    # Configuracoes para Delta Lake
    try:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    except Exception:
        print("[WARN] Delta Lake nao disponivel, usando Parquet")

    # Configuracoes para Azure Storage (se em cloud)
    if settings.is_cloud:
        account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        key = get_secret("AZURE_STORAGE_ACCOUNT_KEY")
        if account and key:
            builder = builder.config(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)

    return builder.getOrCreate()


# Singleton para settings
_settings: Optional[Settings] = None


def settings() -> Settings:
    """Retorna settings como singleton."""
    global _settings
    if _settings is None:
        _settings = get_settings()
    return _settings
