"""
Pytest Configuration and Fixtures

Configuracoes compartilhadas para todos os testes.
"""

import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Cria uma SparkSession para testes.
    Escopo de sessao para reutilizar entre testes.
    """
    # Configuracoes para compatibilidade com Java 17+
    java_options = (
        "-Duser.timezone=UTC "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
        "-Djava.security.manager=allow"
    )
    
    # Define SPARK_LOCAL_IP para evitar warnings
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("ABInBevCase-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
        .config("spark.driver.extraJavaOptions", java_options)
        .config("spark.executor.extraJavaOptions", java_options)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    yield spark

    spark.stop()


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

