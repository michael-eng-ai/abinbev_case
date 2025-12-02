"""
Pytest Configuration and Fixtures

Configuracoes compartilhadas para todos os testes.
"""

import os
import sys

import pytest

# Configura variaveis de ambiente antes de importar PySpark
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    """
    Cria uma SparkSession para testes.
    Escopo de sessao para reutilizar entre testes.
    """
    # Import aqui para garantir que as variaveis de ambiente estao setadas
    from pyspark.sql import SparkSession

    # Configuracoes para compatibilidade com Java 11+
    java_options = "-Duser.timezone=UTC"

    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("ABInBevCase-Tests")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
            .config("spark.driver.extraJavaOptions", java_options)
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )

        # Reduz log level
        spark.sparkContext.setLogLevel("ERROR")

        yield spark

        spark.stop()

    except Exception as e:
        pytest.skip(f"Spark nao disponivel: {e}")


@pytest.fixture
def sample_sales_data(spark):
    """
    Cria dados de vendas de exemplo para testes.
    """
    data = [
        ("REG1", "TG001", "BRAND_A", "202101", 100.0, 10),
        ("REG1", "TG002", "BRAND_B", "202101", 200.0, 20),
        ("REG2", "TG001", "BRAND_A", "202102", 150.0, 15),
        ("REG2", "TG003", "BRAND_C", "202102", 300.0, 30),
    ]

    columns = [
        "btlr_org_lvl_c",
        "trade_group",
        "brand_nm",
        "month_id",
        "hl_qty",
        "transaction_count",
    ]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def sample_channel_data(spark):
    """
    Cria dados de canal de exemplo para testes.
    """
    data = [
        ("TG001", "BAR", 1, 50.0),
        ("TG002", "RESTAURANT", 1, 75.0),
        ("TG003", "SUPERMARKET", 0, 100.0),
    ]

    columns = ["trade_group", "channel_type", "is_premium", "average_order_value"]

    return spark.createDataFrame(data, columns)
