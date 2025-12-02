# Databricks notebook source
# MAGIC %md
# MAGIC # ABInBev Beverage Sales Analytics - Pipeline Completo
# MAGIC 
# MAGIC **Arquitetura Medallion com Delta Lake**
# MAGIC 
# MAGIC Este notebook implementa o pipeline completo de ingestao e transformacao de dados
# MAGIC seguindo a arquitetura Medallion (Bronze -> Silver -> Gold -> Aggregation).
# MAGIC 
# MAGIC ## Requisitos do Case
# MAGIC 1. Data Pipeline: Merge Sales + Channel Features
# MAGIC 2. Modelo Dimensional: Dimensoes + Fatos + Summary Tables
# MAGIC 3. Business Queries: Top 3 Trade Groups, Vendas/Mes, Menor Marca/Regiao

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup e Configuracao

# COMMAND ----------

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)
from pyspark.sql.window import Window
from datetime import datetime
import os

# COMMAND ----------

# Configuracao do Spark Session (para execucao local)
# Em Databricks/HDInsight, o spark ja esta disponivel

def get_spark_session():
    """
    Cria ou obtem SparkSession com Delta Lake configurado.
    """
    try:
        # Em Databricks/HDInsight, spark ja existe
        return spark
    except NameError:
        # Execucao local
        return (SparkSession.builder
            .appName("ABInBev_Beverage_Analytics")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.memory", "4g")
            .getOrCreate())

spark = get_spark_session()
print(f"Spark Version: {spark.version}")

# COMMAND ----------

# Configuracao de Paths
# Detecta automaticamente o diretorio raiz do projeto

# Usa pathlib para obter o caminho relativo ao arquivo atual
# Funciona em qualquer maquina, independente do usuario
BASE_PATH = str(Path(__file__).parent.parent.resolve())

# Paths de dados
SOURCE_PATH = f"{BASE_PATH}/ABI DENG Recrutiment Business Case 1 20210727"
DATA_PATH = f"{BASE_PATH}/data"

# Paths das camadas
BRONZE_PATH = f"{DATA_PATH}/bronze"
SILVER_PATH = f"{DATA_PATH}/silver"
GOLD_PATH = f"{DATA_PATH}/gold"
AGGREGATION_PATH = f"{DATA_PATH}/aggregation"

# Arquivos fonte
SALES_FILE = f"{SOURCE_PATH}/abi_bus_case1_beverage_sales_20210726.csv"
CHANNEL_FILE = f"{SOURCE_PATH}/abi_bus_case1_beverage_channel_group_20210726.csv"

# Batch ID para auditoria
BATCH_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"Batch ID: {BATCH_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze Layer - Ingestao de Dados Brutos

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Ingestao de Vendas (beverage_sales)
# MAGIC 
# MAGIC **IMPORTANTE**: O arquivo esta em UTF-16 com delimitador TAB

# COMMAND ----------

def ingest_beverage_sales(spark, file_path: str, batch_id: str):
    """
    Ingere dados de vendas do CSV (UTF-16, TAB-separated) para Bronze layer.
    Adiciona colunas de auditoria e padroniza nomes.
    
    Args:
        spark: SparkSession
        file_path: Caminho do arquivo CSV
        batch_id: Identificador do lote
    
    Returns:
        DataFrame com dados brutos + auditoria
    """
    print(f"[INFO] Ingerindo beverage_sales de: {file_path}")
    
    # Leitura do CSV com encoding UTF-16
    # PySpark nao suporta UTF-16 diretamente, entao usamos pandas como intermediario
    import pandas as pd
    
    # Le com pandas (suporta UTF-16)
    pdf = pd.read_csv(file_path, encoding='utf-16', sep='\t')
    
    # Converte para Spark DataFrame
    df = spark.createDataFrame(pdf)
    
    # Padroniza nomes das colunas (lowercase, snake_case)
    for col in df.columns:
        new_col = col.lower().replace(" ", "_").replace("$_", "dollar_")
        df = df.withColumnRenamed(col, new_col)
    
    # Adiciona colunas de auditoria
    df_bronze = (df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(file_path))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingestion_date", F.current_date())
    )
    
    print(f"[OK] Registros ingeridos: {df_bronze.count()}")
    print(f"[INFO] Schema:")
    df_bronze.printSchema()
    
    return df_bronze

# Executa ingestao
bronze_sales = ingest_beverage_sales(spark, SALES_FILE, BATCH_ID)

# COMMAND ----------

# Visualizacao dos dados brutos
bronze_sales.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Ingestao de Channel Features

# COMMAND ----------

def ingest_channel_features(spark, file_path: str, batch_id: str):
    """
    Ingere dados de channel features do CSV para Bronze layer.
    
    Args:
        spark: SparkSession
        file_path: Caminho do arquivo CSV
        batch_id: Identificador do lote
    
    Returns:
        DataFrame com dados brutos + auditoria
    """
    print(f"[INFO] Ingerindo channel_features de: {file_path}")
    
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # Padroniza nomes das colunas
    for col in df.columns:
        new_col = col.lower().replace(" ", "_")
        df = df.withColumnRenamed(col, new_col)
    
    # Adiciona colunas de auditoria
    df_bronze = (df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(file_path))
        .withColumn("_batch_id", F.lit(batch_id))
    )
    
    print(f"[OK] Registros ingeridos: {df_bronze.count()}")
    df_bronze.printSchema()
    
    return df_bronze

# Executa ingestao
bronze_channels = ingest_channel_features(spark, CHANNEL_FILE, BATCH_ID)

# COMMAND ----------

# Visualizacao dos dados de canais
bronze_channels.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Persistir Bronze Layer (Delta)

# COMMAND ----------

def save_to_delta(df, table_name: str, path: str):
    """
    Salva DataFrame em formato Delta Lake.
    Fallback para Parquet se Delta nao estiver disponivel.
    
    Args:
        df: DataFrame a salvar
        table_name: Nome da tabela
        path: Caminho base
    """
    full_path = f"{path}/{table_name}"
    print(f"[INFO] Salvando {table_name} em: {full_path}")
    
    try:
        df.write.format("delta").mode("overwrite").save(full_path)
        print(f"[OK] Salvo como Delta Lake")
    except Exception as e:
        df.write.mode("overwrite").parquet(full_path)
        print(f"[OK] Salvo como Parquet (Delta nao disponivel)")

save_to_delta(bronze_sales, "bronze_beverage_sales", BRONZE_PATH)
save_to_delta(bronze_channels, "bronze_channel_features", BRONZE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Layer - Limpeza e Enriquecimento
# MAGIC 
# MAGIC NOTA: Esta secao sera implementada apos aprovacao da arquitetura Bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Layer - Modelo Dimensional
# MAGIC 
# MAGIC NOTA: Esta secao sera implementada apos aprovacao da arquitetura Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Aggregation Layer - Tabelas Resumo
# MAGIC 
# MAGIC NOTA: Esta secao sera implementada apos aprovacao da arquitetura Gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Business Queries
# MAGIC 
# MAGIC NOTA: Esta secao sera implementada apos todas as camadas estarem prontas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Proximos Passos
# MAGIC 
# MAGIC ### Melhorias Tecnicas
# MAGIC 1. Auto Loader: Implementar ingestao incremental
# MAGIC 2. Delta Live Tables: Migrar pipeline para DLT com data quality expectations
# MAGIC 3. SCD Type 2: Implementar versionamento de dimensoes
# MAGIC 4. Unity Catalog: Configurar governanca centralizada
# MAGIC 
# MAGIC ### Melhorias de Qualidade
# MAGIC 1. Great Expectations: Validacoes automatizadas
# MAGIC 2. Data Lineage: Rastreamento completo
# MAGIC 3. Alertas: Notificacoes de falha
# MAGIC 
# MAGIC ### Melhorias de Performance
# MAGIC 1. Z-Order: Otimizacao de queries por regiao/data
# MAGIC 2. Particionamento: Ajustar particoes conforme volume
# MAGIC 3. Caching: Cache estrategico para dimensoes pequenas
