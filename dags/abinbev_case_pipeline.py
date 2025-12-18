"""
ABInBev Case - Apache Airflow DAG

Pipeline de dados orquestrado pelo Airflow seguindo a arquitetura Medallion.
Executa as camadas Bronze -> Silver -> Gold -> Consumption em sequencia.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Configuracoes do projeto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
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
    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home && poetry run python {NOTEBOOKS_PATH}/01_bronze_ingestion.py",
        cwd=str(PROJECT_ROOT),
    )

    # Camada Silver - Transformacao e Qualidade
    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command=f"export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home && poetry run python {NOTEBOOKS_PATH}/02_silver_transformation.py",
        cwd=str(PROJECT_ROOT),
    )

    # Camada Gold - Regras de Negocio
    gold_business_rules = BashOperator(
        task_id="gold_business_rules",
        bash_command=f"export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home && poetry run python {NOTEBOOKS_PATH}/03_gold_business_rules.py",
        cwd=str(PROJECT_ROOT),
    )

    # Camada Consumption - Modelo Dimensional
    consumption_dimensional = BashOperator(
        task_id="consumption_dimensional",
        bash_command=f"export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home && poetry run python {NOTEBOOKS_PATH}/04_consumption_dimensional.py",
        cwd=str(PROJECT_ROOT),
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

