# ==============================================================================
# ABInBev Case - Gold Business Rules
# ==============================================================================
#
# Este notebook/script realiza:
# - Leitura da camada Silver
# - Aplicacao de regras de negocio
# - Adicao de campos calculados
# - Categorizacoes de negocio
# - Persistencia na Gold
#
# Execucao:
# - Local: python notebooks/03_gold_business_rules.py
# - Databricks/HDInsight: Importar como notebook
#
# ==============================================================================

# %% [markdown]
# # Gold Business Rules
# Aplicacao de regras de negocio e enriquecimento dos dados.

# %%
# Imports
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType

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
            "silver": str(project_root / "data" / "silver"),
            "gold": str(project_root / "data" / "gold"),
            "control": str(project_root / "data" / "control"),
        }
    else:
        account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "abinbevdatalake")
        return {
            "silver": f"abfss://silver@{account}.dfs.core.windows.net",
            "gold": f"abfss://gold@{account}.dfs.core.windows.net",
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
        .appName("ABInBev_Gold_Business_Rules")
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
# Funcoes auxiliares

def read_silver(table_name: str) -> DataFrame:
    """Le tabela da Silver."""
    path = f"{PATHS['silver']}/{table_name}"
    print(f"[INFO] Lendo: {path}")
    
    try:
        df = spark.read.format("delta").load(path)
    except Exception:
        df = spark.read.parquet(path)
    
    print(f"[OK] {df.count()} registros lidos")
    return df


def save_gold(df: DataFrame, table_name: str):
    """Salva na Gold."""
    path = f"{PATHS['gold']}/{table_name}"
    print(f"[INFO] Salvando: {path}")
    
    try:
        df.write.format("delta").mode("overwrite").save(path)
        print(f"[OK] Salvo como Delta")
    except Exception:
        df.write.mode("overwrite").parquet(path)
        print(f"[OK] Salvo como Parquet")


# %%
# Regras de Negocio

# Mapeamento de regioes para grupos
REGION_GROUP_MAPPING = {
    "NORTHEAST": "US_EAST",
    "SOUTHEAST": "US_EAST",
    "GREAT LAKES": "US_EAST",
    "MIDWEST": "US_WEST",
    "SOUTHWEST": "US_WEST",
    "WEST": "US_WEST",
    "CANADA": "CANADA"
}

def apply_volume_category(df: DataFrame) -> DataFrame:
    """
    Aplica categorizacao de volume.
    
    Regra:
    - HIGH: dollar_volume > 1000
    - MEDIUM: dollar_volume > 100
    - LOW: dollar_volume <= 100
    """
    return df.withColumn(
        "volume_category",
        F.when(F.col("dollar_volume") > 1000, "HIGH")
         .when(F.col("dollar_volume") > 100, "MEDIUM")
         .otherwise("LOW")
    )


def apply_negative_sale_flag(df: DataFrame) -> DataFrame:
    """
    Identifica vendas negativas (devolucoes/estornos).
    """
    return df.withColumn(
        "is_negative_sale",
        F.when(F.col("dollar_volume") < 0, True).otherwise(False)
    )


def apply_region_group(df: DataFrame) -> DataFrame:
    """
    Aplica agrupamento de regioes.
    
    Mapeamento:
    - US_EAST: NORTHEAST, SOUTHEAST, GREAT LAKES
    - US_WEST: MIDWEST, SOUTHWEST, WEST
    - CANADA: CANADA
    """
    # Cria expressao CASE WHEN
    region_expr = F.lit("OTHER")  # Default
    
    for region, group in REGION_GROUP_MAPPING.items():
        region_expr = F.when(
            F.upper(F.col("region")) == region, group
        ).otherwise(region_expr)
    
    return df.withColumn("region_group", region_expr)


def apply_sales_quarter(df: DataFrame) -> DataFrame:
    """
    Adiciona trimestre baseado no mes.
    """
    return df.withColumn(
        "sales_quarter",
        F.when(F.col("month").isin([1, 2, 3]), "Q1")
         .when(F.col("month").isin([4, 5, 6]), "Q2")
         .when(F.col("month").isin([7, 8, 9]), "Q3")
         .otherwise("Q4")
    )


def apply_weekend_flag(df: DataFrame) -> DataFrame:
    """
    Identifica vendas em fins de semana.
    """
    return df.withColumn(
        "is_weekend_sale",
        F.when(F.dayofweek(F.col("transaction_date")).isin([1, 7]), True)
         .otherwise(False)
    )


def apply_country(df: DataFrame) -> DataFrame:
    """
    Adiciona pais baseado na regiao.
    """
    return df.withColumn(
        "country",
        F.when(F.upper(F.col("region")) == "CANADA", "CANADA")
         .otherwise("USA")
    )


# %%
# Transformacao Gold - Sales

def transform_sales_to_gold(df: DataFrame) -> DataFrame:
    """
    Aplica todas as regras de negocio aos dados de vendas.
    
    Atualiza _updated_at para registrar passagem pela camada.
    Herda _source_file e _ingestion_date para rastreabilidade.
    """
    print("\n[INFO] Aplicando regras de negocio - Sales")
    
    df_gold = df
    
    # Aplica cada regra
    df_gold = apply_volume_category(df_gold)
    print("[OK] volume_category aplicado")
    
    df_gold = apply_negative_sale_flag(df_gold)
    print("[OK] is_negative_sale aplicado")
    
    df_gold = apply_region_group(df_gold)
    print("[OK] region_group aplicado")
    
    df_gold = apply_sales_quarter(df_gold)
    print("[OK] sales_quarter aplicado")
    
    df_gold = apply_weekend_flag(df_gold)
    print("[OK] is_weekend_sale aplicado")
    
    df_gold = apply_country(df_gold)
    print("[OK] country aplicado")
    
    # === AUDITORIA: Atualiza para Gold ===
    df_gold = (df_gold
        .withColumn("_updated_at", F.current_timestamp())
        .withColumn("_layer", F.lit("gold"))
        .withColumn("_gold_timestamp", F.current_timestamp())
    )
    
    print(f"[OK] {df_gold.count()} registros transformados")
    
    return df_gold


# %%
# Transformacao Gold - Channels

def transform_channels_to_gold(df: DataFrame) -> DataFrame:
    """
    Aplica regras de negocio aos dados de canais.
    
    Atualiza _updated_at para registrar passagem pela camada.
    """
    print("\n[INFO] Aplicando regras de negocio - Channels")
    
    df_gold = (df
        # Flag de canal que vende alcool
        .withColumn(
            "is_alcoholic_channel",
            F.when(F.col("trade_type_desc").isin(["ALCOHOLIC", "MIX"]), True)
             .otherwise(False)
        )
        
        # Categoria do canal
        .withColumn(
            "channel_category",
            F.when(F.col("trade_group_desc") == "GROCERY", "RETAIL")
             .when(F.col("trade_group_desc") == "ENTERTAINMENT", "ON_PREMISE")
             .when(F.col("trade_group_desc") == "SERVICES", "CONVENIENCE")
             .otherwise("OTHER")
        )
        
        # === AUDITORIA: Atualiza para Gold ===
        .withColumn("_updated_at", F.current_timestamp())
        .withColumn("_layer", F.lit("gold"))
        .withColumn("_gold_timestamp", F.current_timestamp())
    )
    
    print(f"[OK] {df_gold.count()} registros transformados")
    
    return df_gold


# %%
# Execucao Principal

print("\n" + "=" * 60)
print("GOLD BUSINESS RULES")
print("=" * 60)

# Leitura Silver
silver_sales = read_silver("silver_sales_enriched")
silver_channels = read_silver("silver_channel_features")

# %%
# Transformacoes Gold
gold_sales = transform_sales_to_gold(silver_sales)
gold_channels = transform_channels_to_gold(silver_channels)

# %%
# Visualiza amostra - Sales
print("\n[INFO] Amostra gold_sales_enriched:")
gold_sales.select(
    "sales_id", "brand_nm", "region", "region_group", "country",
    "dollar_volume", "volume_category", "is_negative_sale",
    "sales_quarter", "is_weekend_sale"
).show(10)

# %%
# Visualiza amostra - Channels
print("\n[INFO] Amostra gold_channels_enriched:")
gold_channels.select(
    "trade_chnl_desc", "trade_group_desc", "trade_type_desc",
    "is_alcoholic_channel", "channel_category"
).show(10)

# %%
# Estatisticas das regras de negocio
print("\n[INFO] Estatisticas das categorizacoes:")

print("\nVolume Category:")
gold_sales.groupBy("volume_category").count().show()

print("\nRegion Group:")
gold_sales.groupBy("region_group").count().show()

print("\nSales Quarter:")
gold_sales.groupBy("sales_quarter").count().show()

print("\nNegative Sales:")
gold_sales.groupBy("is_negative_sale").count().show()

# %%
# Persistencia
save_gold(gold_sales, "gold_sales_enriched")
save_gold(gold_channels, "gold_channels_enriched")

# %%
# Resumo
print("\n" + "=" * 60)
print("GOLD BUSINESS RULES - RESUMO")
print("=" * 60)
print(f"Batch ID: {BATCH_ID}")
print(f"Ambiente: {ENVIRONMENT}")
print(f"gold_sales_enriched: {gold_sales.count()} registros")
print(f"gold_channels_enriched: {gold_channels.count()} registros")
print("=" * 60)
print("[OK] Gold business rules concluido com sucesso!")

