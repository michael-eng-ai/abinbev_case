# ==============================================================================
# ABInBev Case - Silver Transformation
# ==============================================================================
#
# Este notebook/script realiza:
# - Leitura da camada Bronze
# - Aplicacao de regras de Data Quality
# - Limpeza e tipagem de dados
# - Enriquecimento (JOIN sales + channels)
# - MERGE INCREMENTAL usando PK + row_hash
# - Persistencia na Silver (dado mais atual) e Quarantine
#
# Padrao CDC:
# - Bronze: Mantem historico completo
# - Silver: Mantem dado mais atual (UPSERT)
#   - Se PK nao existe: INSERT
#   - Se PK existe e row_hash diferente: UPDATE (se _updated_at mais recente)
#   - Se PK existe e row_hash igual: SKIP (sem mudanca)
#
# Execucao:
# - Local: python notebooks/02_silver_transformation.py
# - Databricks/HDInsight: Importar como notebook
#
# ==============================================================================

# %% [markdown]
# # Silver Transformation
# Data Quality, limpeza, enriquecimento e merge incremental.

# %%
# Imports
import os
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Tuple, Dict

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, DecimalType, DateType, BooleanType, StringType
)
from pyspark.sql.window import Window

# %%
# Configuracao
def get_environment():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    return os.getenv("ENVIRONMENT", "local")

ENVIRONMENT = get_environment()
print(f"[INFO] Ambiente: {ENVIRONMENT}")

# %%
# Paths
def get_paths():
    if ENVIRONMENT == "local":
        project_root = Path(__file__).parent.parent if "__file__" in dir() else Path(".")
        return {
            "bronze": str(project_root / "data" / "bronze"),
            "silver": str(project_root / "data" / "silver"),
            "control": str(project_root / "data" / "control"),
        }
    else:
        account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "abinbevdatalake")
        return {
            "bronze": f"abfss://bronze@{account}.dfs.core.windows.net",
            "silver": f"abfss://silver@{account}.dfs.core.windows.net",
            "control": f"abfss://control@{account}.dfs.core.windows.net",
        }

PATHS = get_paths()

# %%
# Spark Session
def get_spark_session():
    try:
        existing = SparkSession.getActiveSession()
        if existing:
            return existing
    except Exception:
        pass
    
    builder = (SparkSession.builder
        .appName("ABInBev_Silver_Transformation")
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
        pass
    
    return builder.getOrCreate()

spark = get_spark_session()
BATCH_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"[INFO] Batch ID: {BATCH_ID}")

# %%
# Process Control - Registro de execucoes
try:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
    from control.process_control import ProcessControl, QuarantineManager
    process_control = ProcessControl(spark, PATHS["control"])
    quarantine_manager = QuarantineManager(spark, PATHS["control"])
    PROCESS_CONTROL_ENABLED = True
    print("[INFO] Process Control habilitado")
except ImportError:
    PROCESS_CONTROL_ENABLED = False
    print("[WARN] Process Control nao disponivel")

# %%
# ==============================================================================
# Funcoes de CDC Merge
# ==============================================================================

def read_bronze(table_name: str) -> DataFrame:
    """Le tabela da Bronze."""
    path = f"{PATHS['bronze']}/{table_name}"
    print(f"[INFO] Lendo Bronze: {path}")
    
    try:
        df = spark.read.format("delta").load(path)
    except Exception:
        df = spark.read.parquet(path)
    
    print(f"[OK] {df.count()} registros lidos")
    return df


def read_silver_if_exists(table_name: str) -> DataFrame:
    """Le tabela da Silver se existir."""
    path = f"{PATHS['silver']}/{table_name}"
    
    try:
        if ENVIRONMENT == "local":
            if not os.path.exists(path):
                return None
        df = spark.read.format("delta").load(path)
        print(f"[INFO] Silver existente: {df.count()} registros")
        return df
    except Exception:
        try:
            df = spark.read.parquet(path)
            print(f"[INFO] Silver existente: {df.count()} registros")
            return df
        except Exception:
            print(f"[INFO] Silver nao existe ainda")
            return None


def merge_incremental(source_df: DataFrame, target_df: DataFrame, 
                      pk_column: str = "_pk") -> Tuple[DataFrame, Dict]:
    """
    Realiza merge incremental usando PK e row_hash.
    
    Logica:
    - NOVOS: PK nao existe no target -> INSERT
    - ALTERADOS: PK existe mas row_hash diferente -> UPDATE (se mais recente)
    - INALTERADOS: PK existe e row_hash igual -> SKIP
    
    Args:
        source_df: DataFrame fonte (Bronze transformado)
        target_df: DataFrame destino (Silver existente)
        pk_column: Nome da coluna de PK
        
    Returns:
        Tuple[DataFrame merged, Dict stats]
    """
    print("\n[INFO] Iniciando merge incremental...")
    
    stats = {
        "source_count": source_df.count(),
        "target_count": target_df.count() if target_df else 0,
        "inserted": 0,
        "updated": 0,
        "unchanged": 0
    }
    
    # Se nao existe target, tudo e INSERT
    if target_df is None or stats["target_count"] == 0:
        stats["inserted"] = stats["source_count"]
        print(f"[OK] Primeira carga: {stats['inserted']} INSERTs")
        return source_df, stats
    
    # Join para comparar
    source_alias = source_df.alias("source")
    target_alias = target_df.alias("target")
    
    # Registros NOVOS (PK nao existe no target)
    new_records = source_alias.join(
        target_alias,
        F.col(f"source.{pk_column}") == F.col(f"target.{pk_column}"),
        how="left_anti"
    )
    stats["inserted"] = new_records.count()
    
    # Registros que existem em ambos (para comparar hash)
    matched = source_alias.join(
        target_alias.select(pk_column, "_row_hash", "_updated_at"),
        F.col(f"source.{pk_column}") == F.col(f"target.{pk_column}"),
        how="inner"
    )
    
    # ALTERADOS: hash diferente E source mais recente
    changed = matched.filter(
        (F.col("source._row_hash") != F.col("target._row_hash")) &
        (F.col("source._updated_at") >= F.col("target._updated_at"))
    ).select("source.*")
    stats["updated"] = changed.count()
    
    # INALTERADOS: hash igual OU target mais recente
    unchanged_from_target = target_alias.join(
        source_alias.select(pk_column, "_row_hash", "_updated_at"),
        F.col(f"target.{pk_column}") == F.col(f"source.{pk_column}"),
        how="inner"
    ).filter(
        (F.col("target._row_hash") == F.col("source._row_hash")) |
        (F.col("target._updated_at") > F.col("source._updated_at"))
    ).select("target.*")
    stats["unchanged"] = unchanged_from_target.count()
    
    # Registros que so existem no target (manter)
    only_in_target = target_alias.join(
        source_alias.select(pk_column),
        F.col(f"target.{pk_column}") == F.col(f"source.{pk_column}"),
        how="left_anti"
    )
    
    # Monta resultado final
    # NOVOS + ALTERADOS (do source) + INALTERADOS (do target) + SO_NO_TARGET
    result = (new_records
              .unionByName(changed, allowMissingColumns=True)
              .unionByName(unchanged_from_target, allowMissingColumns=True)
              .unionByName(only_in_target, allowMissingColumns=True))
    
    # Remove duplicatas por PK (mantem mais recente)
    window = Window.partitionBy(pk_column).orderBy(F.col("_updated_at").desc())
    result = (result
              .withColumn("_rn", F.row_number().over(window))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))
    
    print(f"[OK] Merge stats:")
    print(f"     INSERTs (novos):     {stats['inserted']}")
    print(f"     UPDATEs (alterados): {stats['updated']}")
    print(f"     SKIPs (inalterados): {stats['unchanged']}")
    print(f"     Total resultado:     {result.count()}")
    
    return result, stats


def save_silver(df: DataFrame, table_name: str):
    """Salva na Silver."""
    path = f"{PATHS['silver']}/{table_name}"
    print(f"[INFO] Salvando Silver: {path}")
    
    try:
        df.write.format("delta").mode("overwrite").save(path)
        print(f"[OK] Salvo como Delta")
    except Exception:
        df.write.mode("overwrite").parquet(path)
        print(f"[OK] Salvo como Parquet")


def save_quarantine(df: DataFrame, source_table: str, target_table: str, 
                    error_code: str, error_desc: str, batch_id: str):
    """Salva registros na quarentena."""
    if df.count() == 0:
        return
    
    path = f"{PATHS['control']}/quarantine"
    
    quarantine_df = (df
        .withColumn("quarantine_id", F.expr("uuid()"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_table", F.lit(source_table))
        .withColumn("target_table", F.lit(target_table))
        .withColumn("record_data", F.to_json(F.struct(*df.columns)))
        .withColumn("error_type", F.lit("VALIDATION_ERROR"))
        .withColumn("error_code", F.lit(error_code))
        .withColumn("error_description", F.lit(error_desc))
        .withColumn("dq_rule_name", F.lit(error_code))
        .withColumn("is_known_rule", F.lit(True))
        .withColumn("reprocessed", F.lit(False))
        .withColumn("reprocess_batch_id", F.lit(None).cast(StringType()))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "quarantine_id", "batch_id", "source_table", "target_table",
            "record_data", "error_type", "error_code", "error_description",
            "dq_rule_name", "is_known_rule", "reprocessed", "reprocess_batch_id",
            "created_at", "updated_at"
        )
    )
    
    try:
        quarantine_df.write.format("delta").mode("append").save(path)
    except Exception:
        quarantine_df.write.mode("append").parquet(path)
    
    print(f"[WARN] {quarantine_df.count()} registros para quarentena: {error_code}")


# %%
# ==============================================================================
# Regras de Data Quality
# ==============================================================================

def apply_dq_sales(df: DataFrame) -> DataFrame:
    """
    Aplica regras de Data Quality para sales.
    """
    print("\n[INFO] Aplicando regras de Data Quality - Sales")
    
    total = df.count()
    
    # DQ_SALES_001: brand_nm NOT NULL
    null_brand = df.filter(F.col("brand_nm").isNull())
    if null_brand.count() > 0:
        save_quarantine(null_brand, "bronze_beverage_sales", "silver_beverage_sales",
                       "DQ_SALES_001", "brand_nm nao pode ser nulo", BATCH_ID)
        df = df.filter(F.col("brand_nm").isNotNull())
    
    # DQ_SALES_002: trade_chnl_desc NOT NULL
    null_channel = df.filter(F.col("trade_chnl_desc").isNull())
    if null_channel.count() > 0:
        save_quarantine(null_channel, "bronze_beverage_sales", "silver_beverage_sales",
                       "DQ_SALES_002", "trade_chnl_desc nao pode ser nulo", BATCH_ID)
        df = df.filter(F.col("trade_chnl_desc").isNotNull())
    
    # DQ_SALES_005: TRIM whitespace
    df = df.withColumn("brand_nm", F.trim(F.col("brand_nm")))
    df = df.withColumn("trade_chnl_desc", F.trim(F.col("trade_chnl_desc")))
    df = df.withColumn("btlr_org_lvl_c_desc", F.trim(F.col("btlr_org_lvl_c_desc")))
    
    valid_count = df.count()
    invalid_count = total - valid_count
    
    print(f"[OK] Validos: {valid_count} | Quarentena: {invalid_count}")
    
    return df


# %%
# ==============================================================================
# Transformacoes Silver
# ==============================================================================

def transform_sales_to_silver(df: DataFrame) -> DataFrame:
    """
    Transforma dados de vendas para Silver.
    
    Atualiza _updated_at para registrar passagem pela camada.
    Herda _source_file e _ingestion_date para rastreabilidade.
    """
    print("\n[INFO] Transformando sales para Silver")
    
    df_silver = (df
        # Gera ID unico
        .withColumn("sales_id", F.monotonically_increasing_id())
        
        # Converte data
        .withColumn("transaction_date", F.to_date(F.col("date"), "M/d/yyyy"))
        
        # Converte tipos
        .withColumn("year", F.col("year").cast(IntegerType()))
        .withColumn("month", F.col("month").cast(IntegerType()))
        .withColumn("period", F.col("period").cast(IntegerType()))
        .withColumn("dollar_volume", F.col("dollar_volume").cast(DecimalType(18, 2)))
        
        # Campos derivados
        .withColumn("year_month", F.concat(
            F.col("year").cast(StringType()),
            F.lit("-"),
            F.lpad(F.col("month").cast(StringType()), 2, "0")
        ))
        
        # Renomeia para padrao
        .withColumnRenamed("btlr_org_lvl_c_desc", "region")
        
        # === AUDITORIA: Atualiza para Silver ===
        # Atualiza _updated_at (nova passagem de camada)
        .withColumn("_updated_at", F.current_timestamp())
        # Atualiza _layer para silver
        .withColumn("_layer", F.lit("silver"))
        # Adiciona timestamp de processamento Silver
        .withColumn("_silver_timestamp", F.current_timestamp())
        # Herda _source_file e _ingestion_date automaticamente
        
        # Remove coluna date original
        .drop("date")
    )
    
    print(f"[OK] {df_silver.count()} registros transformados")
    
    return df_silver


def transform_channels_to_silver(df: DataFrame) -> DataFrame:
    """
    Transforma dados de canais para Silver.
    
    Atualiza _updated_at para registrar passagem pela camada.
    """
    print("\n[INFO] Transformando channels para Silver")
    
    df_silver = (df
        .withColumn("channel_sk", F.monotonically_increasing_id())
        .withColumn("trade_chnl_desc", F.trim(F.col("trade_chnl_desc")))
        .withColumn("trade_group_desc", F.trim(F.col("trade_group_desc")))
        .withColumn("trade_type_desc", F.trim(F.col("trade_type_desc")))
        # === AUDITORIA: Atualiza para Silver ===
        .withColumn("_updated_at", F.current_timestamp())
        .withColumn("_layer", F.lit("silver"))
        .withColumn("_silver_timestamp", F.current_timestamp())
    )
    
    print(f"[OK] {df_silver.count()} registros transformados")
    
    return df_silver


def enrich_sales_with_channels(sales_df: DataFrame, channels_df: DataFrame) -> DataFrame:
    """Enriquece vendas com dados de canal."""
    print("\n[INFO] Enriquecendo sales com channels")
    
    channels_for_join = channels_df.select(
        "trade_chnl_desc",
        "trade_group_desc",
        "trade_type_desc"
    )
    
    enriched = sales_df.join(
        channels_for_join,
        on="trade_chnl_desc",
        how="left"
    )
    
    orphans = enriched.filter(F.col("trade_group_desc").isNull()).count()
    if orphans > 0:
        print(f"[WARN] {orphans} registros sem match de canal")
    
    print(f"[OK] {enriched.count()} registros enriquecidos")
    
    return enriched


# %%
# ==============================================================================
# Execucao Principal
# ==============================================================================

print("\n" + "=" * 60)
print("SILVER TRANSFORMATION (INCREMENTAL)")
print("=" * 60)

# Leitura Bronze
bronze_sales = read_bronze("bronze_beverage_sales")
bronze_channels = read_bronze("bronze_channel_features")

# %%
# Data Quality
sales_valid = apply_dq_sales(bronze_sales)

# %%
# Transformacoes
silver_sales_transformed = transform_sales_to_silver(sales_valid)
silver_channels_transformed = transform_channels_to_silver(bronze_channels)

# %%
# Enriquecimento
silver_sales_enriched = enrich_sales_with_channels(silver_sales_transformed, silver_channels_transformed)

# %%
# ==============================================================================
# MERGE INCREMENTAL
# ==============================================================================

# Le Silver existente (se houver)
existing_sales = read_silver_if_exists("silver_sales_enriched")
existing_channels = read_silver_if_exists("silver_channel_features")

# Merge Sales
print("\n--- MERGE: silver_sales_enriched ---")
merged_sales, sales_stats = merge_incremental(silver_sales_enriched, existing_sales, "_pk")

# Merge Channels
print("\n--- MERGE: silver_channel_features ---")
merged_channels, channels_stats = merge_incremental(silver_channels_transformed, existing_channels, "_pk")

# %%
# Visualiza amostra
print("\n[INFO] Amostra silver_sales_enriched:")
merged_sales.select(
    "_pk", "_row_hash", "_updated_at",
    "sales_id", "transaction_date", "brand_nm", "region",
    "trade_chnl_desc", "trade_group_desc", "dollar_volume"
).show(10, truncate=False)

# %%
# Persistencia (sempre overwrite com dados merged)
save_silver(merged_sales, "silver_sales_enriched")
save_silver(merged_channels, "silver_channel_features")

# %%
# ==============================================================================
# Registro no Process Control
# ==============================================================================

sales_final_count = merged_sales.count()
channels_final_count = merged_channels.count()

# Conta registros em quarentena (aproximado baseado nas stats)
quarantine_count = sales_stats.get('source_count', 0) - sales_stats.get('inserted', 0) - sales_stats.get('updated', 0) - sales_stats.get('unchanged', 0)
quarantine_count = max(0, quarantine_count)

if PROCESS_CONTROL_ENABLED:
    # Registra processamento de sales
    process_control.start_process(BATCH_ID, "silver", "silver_sales_enriched")
    process_control.end_process(
        status="SUCCESS",
        records_read=sales_stats.get('source_count', 0),
        records_written=sales_final_count,
        records_quarantined=quarantine_count,
        records_failed=0
    )
    
    # Registra processamento de channels
    process_control.start_process(BATCH_ID, "silver", "silver_channel_features")
    process_control.end_process(
        status="SUCCESS",
        records_read=channels_stats.get('source_count', 0),
        records_written=channels_final_count,
        records_quarantined=0,
        records_failed=0
    )

# %%
# ==============================================================================
# Resumo
# ==============================================================================

print("\n" + "=" * 60)
print("SILVER TRANSFORMATION - RESUMO")
print("=" * 60)
print(f"Batch ID: {BATCH_ID}")
print(f"Ambiente: {ENVIRONMENT}")
print(f"\nsilver_sales_enriched:")
print(f"  Total final: {sales_final_count} registros")
print(f"  INSERTs:     {sales_stats['inserted']}")
print(f"  UPDATEs:     {sales_stats['updated']}")
print(f"  SKIPs:       {sales_stats['unchanged']}")
print(f"\nsilver_channel_features:")
print(f"  Total final: {channels_final_count} registros")
print(f"  INSERTs:     {channels_stats['inserted']}")
print(f"  UPDATEs:     {channels_stats['updated']}")
print(f"  SKIPs:       {channels_stats['unchanged']}")
print(f"\nProcess Control: {'Registrado' if PROCESS_CONTROL_ENABLED else 'Desabilitado'}")
print("=" * 60)
print("[OK] Silver transformation concluida com sucesso!")
