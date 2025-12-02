"""
ABInBev Case - Apache Airflow DAG

Pipeline de dados orquestrado pelo Airflow seguindo a arquitetura Medallion.
Executa as camadas Bronze -> Silver -> Gold -> Consumption em sequencia.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

# Configuracoes do projeto
PROJECT_ROOT = Path(__file__).parent.parent
NOTEBOOKS_PATH = PROJECT_ROOT / "notebooks"

# Argumentos padrao para todas as tasks
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# Configuracoes do Spark
spark_config = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
}


def check_source_files(**context):
    """
    Verifica se os arquivos fonte estao disponiveis.
    """
    import os

    source_path = PROJECT_ROOT / "ABI DENG Recrutiment Business Case 1 20210727"
    required_files = [
        "abi_bus_case1_beverage_sales_20210726.csv",
        "abi_bus_case1_beverage_channel_group_20210726.csv",
    ]

    for file_name in required_files:
        file_path = source_path / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"Arquivo fonte nao encontrado: {file_path}")

    return True


with DAG(
    dag_id="abinbev_case_medallion",
    description="Pipeline Medallion para o case ABInBev",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["abinbev", "case", "medallion", "delta-lake"],
) as dag:

    # Task inicial - verifica pre-requisitos
    start = EmptyOperator(task_id="start")

    check_files = PythonOperator(
        task_id="check_source_files",
        python_callable=check_source_files,
    )

    # Camada Bronze - Ingestao
    bronze_ingestion = SparkSubmitOperator(
        task_id="bronze_ingestion",
        application=str(NOTEBOOKS_PATH / "01_bronze_ingestion.py"),
        conn_id="spark_default",
        conf=spark_config,
        name="bronze_ingestion",
        verbose=True,
    )

    # Camada Silver - Transformacao e Qualidade
    silver_transformation = SparkSubmitOperator(
        task_id="silver_transformation",
        application=str(NOTEBOOKS_PATH / "02_silver_transformation.py"),
        conn_id="spark_default",
        conf=spark_config,
        name="silver_transformation",
        verbose=True,
    )

    # Camada Gold - Regras de Negocio
    gold_business_rules = SparkSubmitOperator(
        task_id="gold_business_rules",
        application=str(NOTEBOOKS_PATH / "03_gold_business_rules.py"),
        conn_id="spark_default",
        conf=spark_config,
        name="gold_business_rules",
        verbose=True,
    )

    # Camada Consumption - Modelo Dimensional
    consumption_dimensional = SparkSubmitOperator(
        task_id="consumption_dimensional",
        application=str(NOTEBOOKS_PATH / "04_consumption_dimensional.py"),
        conn_id="spark_default",
        conf=spark_config,
        name="consumption_dimensional",
        verbose=True,
    )

    # Task final
    end = EmptyOperator(task_id="end")

    # Definicao do fluxo
    (
        start
        >> check_files
        >> bronze_ingestion
        >> silver_transformation
        >> gold_business_rules
        >> consumption_dimensional
        >> end
    )

