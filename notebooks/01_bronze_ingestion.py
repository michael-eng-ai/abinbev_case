# ==============================================================================
# ABInBev Case - Bronze Ingestion
# ==============================================================================
#
# Este notebook/script realiza a ingestao dos dados da Landing para Bronze.
#
# Funcionalidades:
# - Leitura de arquivos CSV (UTF-16 e UTF-8)
# - Padronizacao de nomes de colunas
# - Geracao de PK (Primary Key) para identificacao unica
# - Geracao de row_hash para deteccao de mudancas (CDC)
# - Adicao de campos de auditoria
# - Persistencia em Delta Lake (append para historico)
#
# Padrao CDC:
# - Bronze: Mantem historico completo (append)
# - Silver: Mantem dado mais atual (upsert via PK + hash)
#
# Execucao:
# - Local: python notebooks/01_bronze_ingestion.py
# - Databricks/HDInsight: Importar como notebook
#
# ==============================================================================

# %% [markdown]
# # Bronze Ingestion
# Ingestao de dados da Landing Zone para a camada Bronze.
# Inclui geracao de PK e row_hash para processamento incremental.

# %%
# Imports
import os
import sys
from datetime import datetime
from pathlib import Path

# Adiciona src ao path para imports locais
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# %%
# Configuracao - Detecta ambiente
def get_environment():
    """Detecta se esta rodando local ou em cloud."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    return os.getenv("ENVIRONMENT", "local")

ENVIRONMENT = get_environment()
print(f"[INFO] Ambiente: {ENVIRONMENT}")

# %%
# Configuracao de paths
def get_paths():
    """Retorna paths baseados no ambiente."""
    if ENVIRONMENT == "local":
        project_root = Path(__file__).parent.parent if "__file__" in dir() else Path(".")
        return {
            "source": str(project_root / "ABI DENG Recrutiment Business Case 1 20210727"),
            "landing": str(project_root / "data" / "landing"),
            "bronze": str(project_root / "data" / "bronze"),
            "control": str(project_root / "data" / "control"),
        }
    else:
        account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "abinbevdatalake")
        return {
            "source": f"abfss://landing@{account}.dfs.core.windows.net/source",
            "landing": f"abfss://landing@{account}.dfs.core.windows.net",
            "bronze": f"abfss://bronze@{account}.dfs.core.windows.net",
            "control": f"abfss://control@{account}.dfs.core.windows.net",
        }

PATHS = get_paths()
print(f"[INFO] Paths configurados: {PATHS}")

# %%
# Spark Session
def get_spark_session():
    """Obtem ou cria SparkSession."""
    try:
        existing = SparkSession.getActiveSession()
        if existing:
            print("[INFO] Usando SparkSession existente")
            return existing
    except Exception:
        pass
    
    print("[INFO] Criando nova SparkSession")
    
    builder = (SparkSession.builder
        .appName("ABInBev_Bronze_Ingestion")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.adaptive.enabled", "true")
    )
    
    try:
        builder = (builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", 
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    except Exception:
        print("[WARN] Delta Lake nao disponivel")
    
    if ENVIRONMENT != "local":
        account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        if account and key:
            builder = builder.config(
                f"fs.azure.account.key.{account}.dfs.core.windows.net",
                key
            )
    
    return builder.getOrCreate()

spark = get_spark_session()
print(f"[INFO] Spark version: {spark.version}")

# %%
# Batch ID para auditoria
BATCH_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"[INFO] Batch ID: {BATCH_ID}")

# %%
# ==============================================================================
# Funcoes de CDC (Change Data Capture)
# ==============================================================================

def generate_pk(df: DataFrame, pk_columns: list) -> DataFrame:
    """
    Gera Primary Key como hash MD5 das colunas de identificacao.
    
    Args:
        df: DataFrame
        pk_columns: Lista de colunas que compoem a PK
        
    Returns:
        DataFrame com coluna _pk adicionada
    """
    # Concatena colunas PK com separador
    pk_expr = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) 
                                  for c in pk_columns])
    
    # Gera hash MD5
    return df.withColumn("_pk", F.md5(pk_expr))


def generate_row_hash(df: DataFrame, exclude_columns: list = None) -> DataFrame:
    """
    Gera hash de todas as colunas de negocio para deteccao de mudancas.
    
    Args:
        df: DataFrame
        exclude_columns: Colunas a excluir do hash (audit columns)
        
    Returns:
        DataFrame com coluna _row_hash adicionada
    """
    if exclude_columns is None:
        exclude_columns = []
    
    # Colunas de auditoria padrao a excluir
    audit_cols = ["_pk", "_row_hash", "_ingestion_timestamp", "_source_file", 
                  "_batch_id", "_ingestion_date", "_updated_at"]
    exclude_columns.extend(audit_cols)
    
    # Seleciona apenas colunas de negocio
    business_cols = [c for c in df.columns if c not in exclude_columns]
    
    # Concatena todas as colunas
    hash_expr = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) 
                                    for c in sorted(business_cols)])
    
    # Gera hash SHA256 (mais robusto que MD5 para deteccao de mudancas)
    return df.withColumn("_row_hash", F.sha2(hash_expr, 256))


# %%
# Funcoes de padronizacao

def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Padroniza nomes de colunas para snake_case lowercase.
    """
    for col_name in df.columns:
        new_name = (col_name
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("$_", "dollar_")
            .replace("$", "dollar_")
        )
        df = df.withColumnRenamed(col_name, new_name)
    
    return df


def add_audit_columns(df: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    """
    Adiciona colunas de auditoria para rastreabilidade completa.
    
    Campos:
    - _source_file: Arquivo de origem (para rastreabilidade)
    - _ingestion_date: Data da ingestao (para saber quando foi ingerido)
    - _ingestion_timestamp: Timestamp exato da ingestao
    - _updated_at: Timestamp da ultima atualizacao (atualizado em cada camada)
    - _batch_id: ID do lote de processamento
    - _layer: Camada atual (bronze)
    """
    # Extrai apenas o nome do arquivo (sem path completo)
    file_name = source_file.split("/")[-1] if "/" in source_file else source_file
    
    return (df
        .withColumn("_source_file", F.lit(file_name))
        .withColumn("_source_path", F.lit(source_file))
        .withColumn("_ingestion_date", F.current_date())
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_updated_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_layer", F.lit("bronze"))
    )


def save_to_bronze(df: DataFrame, table_name: str, path: str, mode: str = "append"):
    """
    Salva DataFrame na Bronze Layer.
    
    Modos:
    - append: Adiciona novos registros (padrao para historico)
    - overwrite: Substitui tudo (apenas para carga inicial)
    
    Bronze mantem historico completo para rastreabilidade.
    """
    full_path = f"{path}/{table_name}"
    print(f"[INFO] Salvando {table_name} em: {full_path} (mode={mode})")
    
    try:
        df.write.format("delta").mode(mode).save(full_path)
        print(f"[OK] Salvo como Delta Lake")
    except Exception as e:
        print(f"[WARN] Delta nao disponivel: {e}")
        df.write.mode(mode).parquet(full_path)
        print(f"[OK] Salvo como Parquet")


# %%
# ==============================================================================
# Ingestao: beverage_sales (UTF-16, TAB)
# ==============================================================================

# Definicao da PK para beverage_sales
# PK = combinacao unica de: produto + regiao + canal + periodo
SALES_PK_COLUMNS = [
    "ce_brand_flvr",    # Produto/Marca
    "region",           # Regiao
    "trade_chnl_desc",  # Canal
    "year",             # Ano
    "month"             # Mes
]

def ingest_beverage_sales():
    """Ingere dados de vendas com PK e row_hash."""
    file_path = f"{PATHS['source']}/abi_bus_case1_beverage_sales_20210726.csv"
    print(f"\n[INFO] Ingerindo beverage_sales de: {file_path}")
    
    # UTF-16 nao suportado diretamente pelo Spark
    import pandas as pd
    
    if ENVIRONMENT == "local":
        pdf = pd.read_csv(file_path, encoding='utf-16', sep='\t')
        df = spark.createDataFrame(pdf)
    else:
        df = spark.read.csv(file_path, header=True, inferSchema=True, sep='\t')
    
    # 1. Padroniza nomes
    df = standardize_column_names(df)
    
    # 2. Gera PK (antes de adicionar audit columns)
    df = generate_pk(df, SALES_PK_COLUMNS)
    print(f"[OK] PK gerada com colunas: {SALES_PK_COLUMNS}")
    
    # 3. Gera row_hash (para detectar mudancas)
    df = generate_row_hash(df)
    print(f"[OK] row_hash gerado")
    
    # 4. Adiciona auditoria
    df = add_audit_columns(df, file_path, BATCH_ID)
    
    # Reordena colunas (audit first)
    audit_cols = ["_pk", "_row_hash", "_source_file", "_source_path", 
                  "_ingestion_date", "_ingestion_timestamp", "_updated_at",
                  "_batch_id", "_layer"]
    business_cols = [c for c in df.columns if c not in audit_cols]
    df = df.select(audit_cols + business_cols)
    
    print(f"[OK] Registros ingeridos: {df.count()}")
    print(f"[INFO] Schema:")
    df.printSchema()
    
    return df


bronze_sales = ingest_beverage_sales()

# %%
# Visualiza amostra com PK e hash
print("\n[INFO] Amostra com PK e row_hash:")
bronze_sales.select("_pk", "_row_hash", "ce_brand_flvr", "region", 
                     "trade_chnl_desc", "year", "month", "dollar_volume").show(5, truncate=False)

# %%
# ==============================================================================
# Ingestao: channel_features (UTF-8, COMMA)
# ==============================================================================

# Definicao da PK para channel_features
# PK = trade_chnl_desc (identificador unico do canal)
CHANNEL_PK_COLUMNS = ["trade_chnl_desc"]

def ingest_channel_features():
    """Ingere dados de canais com PK e row_hash."""
    file_path = f"{PATHS['source']}/abi_bus_case1_beverage_channel_group_20210726.csv"
    print(f"\n[INFO] Ingerindo channel_features de: {file_path}")
    
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # 1. Padroniza nomes
    df = standardize_column_names(df)
    
    # 2. Gera PK
    df = generate_pk(df, CHANNEL_PK_COLUMNS)
    print(f"[OK] PK gerada com colunas: {CHANNEL_PK_COLUMNS}")
    
    # 3. Gera row_hash
    df = generate_row_hash(df)
    print(f"[OK] row_hash gerado")
    
    # 4. Adiciona auditoria
    df = add_audit_columns(df, file_path, BATCH_ID)
    
    # Reordena colunas
    audit_cols = ["_pk", "_row_hash", "_source_file", "_source_path", 
                  "_ingestion_date", "_ingestion_timestamp", "_updated_at",
                  "_batch_id", "_layer"]
    business_cols = [c for c in df.columns if c not in audit_cols]
    df = df.select(audit_cols + business_cols)
    
    print(f"[OK] Registros ingeridos: {df.count()}")
    df.printSchema()
    
    return df


bronze_channels = ingest_channel_features()

# %%
# Visualiza amostra
print("\n[INFO] Amostra com PK e row_hash:")
bronze_channels.select("_pk", "_row_hash", "trade_chnl_desc", 
                        "trade_group_desc", "trade_type_desc").show(truncate=False)

# %%
# ==============================================================================
# Persistencia na Bronze (APPEND para historico)
# ==============================================================================

# Primeira execucao: overwrite para criar tabela
# Execucoes subsequentes: append para manter historico
# Em producao, usar upsert com Delta Lake merge

# Verifica se e primeira execucao
import os
sales_path = f"{PATHS['bronze']}/bronze_beverage_sales"
channels_path = f"{PATHS['bronze']}/bronze_channel_features"

is_first_run = not os.path.exists(sales_path) if ENVIRONMENT == "local" else True

mode = "overwrite" if is_first_run else "append"
print(f"\n[INFO] Modo de persistencia: {mode}")

save_to_bronze(bronze_sales, "bronze_beverage_sales", PATHS["bronze"], mode=mode)
save_to_bronze(bronze_channels, "bronze_channel_features", PATHS["bronze"], mode=mode)

# %%
# ==============================================================================
# Validacao de PKs
# ==============================================================================

print("\n[INFO] Validacao de PKs:")

# Verifica unicidade da PK em sales
pk_count_sales = bronze_sales.select("_pk").distinct().count()
total_sales = bronze_sales.count()
print(f"  bronze_beverage_sales:")
print(f"    Total registros: {total_sales}")
print(f"    PKs unicas: {pk_count_sales}")
print(f"    Duplicatas: {total_sales - pk_count_sales}")

# Verifica unicidade da PK em channels
pk_count_channels = bronze_channels.select("_pk").distinct().count()
total_channels = bronze_channels.count()
print(f"  bronze_channel_features:")
print(f"    Total registros: {total_channels}")
print(f"    PKs unicas: {pk_count_channels}")
print(f"    Duplicatas: {total_channels - pk_count_channels}")

# %%
# Resumo

print("\n" + "=" * 60)
print("BRONZE INGESTION - RESUMO")
print("=" * 60)
print(f"Batch ID: {BATCH_ID}")
print(f"Ambiente: {ENVIRONMENT}")
print(f"Modo: {mode}")
print(f"\nbronze_beverage_sales:")
print(f"  Registros: {bronze_sales.count()}")
print(f"  PK columns: {SALES_PK_COLUMNS}")
print(f"\nbronze_channel_features:")
print(f"  Registros: {bronze_channels.count()}")
print(f"  PK columns: {CHANNEL_PK_COLUMNS}")
print(f"\nCampos de CDC adicionados:")
print(f"  _pk: Hash MD5 das colunas de identificacao")
print(f"  _row_hash: Hash SHA256 das colunas de negocio")
print(f"  _updated_at: Timestamp de atualizacao")
print("=" * 60)
print("[OK] Bronze ingestion concluida com sucesso!")
