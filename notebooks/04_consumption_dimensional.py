# ==============================================================================
# ABInBev Case - Consumption Dimensional Model
# ==============================================================================
#
# Este notebook/script realiza:
# - Leitura da camada Gold
# - Criacao de dimensoes (dim_date, dim_region, dim_product, dim_channel)
# - Criacao de tabela fato (fact_sales)
# - Criacao de tabelas agregadas (respostas para perguntas de negocio)
# - Persistencia na Consumption
#
# Execucao:
# - Local: python notebooks/04_consumption_dimensional.py
# - Databricks/HDInsight: Importar como notebook
#
# ==============================================================================

# %% [markdown]
# # Consumption Dimensional Model
# Modelo dimensional (Star Schema) para consumo por BI e areas de negocio.

# %%
# Imports
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

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
            "gold": str(project_root / "data" / "gold"),
            "consumption": str(project_root / "data" / "consumption"),
        }
    else:
        account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "abinbevdatalake")
        return {
            "gold": f"abfss://gold@{account}.dfs.core.windows.net",
            "consumption": f"abfss://consumption@{account}.dfs.core.windows.net",
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
        .appName("ABInBev_Consumption_Dimensional")
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

def read_gold(table_name: str) -> DataFrame:
    """Le tabela da Gold."""
    path = f"{PATHS['gold']}/{table_name}"
    print(f"[INFO] Lendo: {path}")
    
    try:
        df = spark.read.format("delta").load(path)
    except Exception:
        df = spark.read.parquet(path)
    
    print(f"[OK] {df.count()} registros lidos")
    return df


def save_consumption(df: DataFrame, table_name: str):
    """Salva na Consumption."""
    path = f"{PATHS['consumption']}/{table_name}"
    print(f"[INFO] Salvando: {path}")
    
    try:
        df.write.format("delta").mode("overwrite").save(path)
        print(f"[OK] Salvo como Delta")
    except Exception:
        df.write.mode("overwrite").parquet(path)
        print(f"[OK] Salvo como Parquet")


# %%
# Criacao de Dimensoes

def add_consumption_audit(df: DataFrame, table_name: str) -> DataFrame:
    """
    Adiciona campos de auditoria para camada Consumption.
    """
    return (df
        .withColumn("_updated_at", F.current_timestamp())
        .withColumn("_layer", F.lit("consumption"))
        .withColumn("_consumption_timestamp", F.current_timestamp())
        .withColumn("_table_name", F.lit(table_name))
    )


def create_dim_date(sales_df: DataFrame) -> DataFrame:
    """
    Cria dimensao de data.
    """
    print("\n[INFO] Criando dim_date")
    
    dates = sales_df.select(
        "transaction_date", "year", "month", "sales_quarter"
    ).distinct()
    
    dim_date = (dates
        .withColumn("date_key", 
            F.date_format(F.col("transaction_date"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("full_date", F.col("transaction_date"))
        .withColumn("quarter", F.quarter(F.col("transaction_date")))
        .withColumn("quarter_name", F.col("sales_quarter"))
        .withColumn("month_name", F.date_format(F.col("transaction_date"), "MMMM"))
        .withColumn("week_of_year", F.weekofyear(F.col("transaction_date")))
        .withColumn("day_of_month", F.dayofmonth(F.col("transaction_date")))
        .withColumn("day_of_week", F.dayofweek(F.col("transaction_date")))
        .withColumn("day_name", F.date_format(F.col("transaction_date"), "EEEE"))
        .withColumn("is_weekend", 
            F.when(F.dayofweek(F.col("transaction_date")).isin([1, 7]), True)
             .otherwise(False))
        .withColumn("year_month", F.concat(
            F.col("year").cast("string"),
            F.lit("-"),
            F.lpad(F.col("month").cast("string"), 2, "0")
        ))
        .select(
            "date_key", "full_date", "year", "quarter", "quarter_name",
            "month", "month_name", "week_of_year", "day_of_month",
            "day_of_week", "day_name", "is_weekend", "year_month"
        )
        .distinct()
    )
    
    # Adiciona auditoria
    dim_date = add_consumption_audit(dim_date, "dim_date")
    
    print(f"[OK] {dim_date.count()} registros criados")
    return dim_date


def create_dim_region(sales_df: DataFrame) -> DataFrame:
    """
    Cria dimensao de regiao.
    """
    print("\n[INFO] Criando dim_region")
    
    regions = sales_df.select(
        "region", "region_group", "country"
    ).distinct()
    
    dim_region = (regions
        .withColumn("region_key", F.monotonically_increasing_id().cast(IntegerType()) + 1)
        .withColumn("region_name", F.col("region"))
        .select("region_key", "region_name", "region_group", "country")
    )
    
    # Adiciona auditoria
    dim_region = add_consumption_audit(dim_region, "dim_region")
    
    print(f"[OK] {dim_region.count()} registros criados")
    return dim_region


def create_dim_product(sales_df: DataFrame) -> DataFrame:
    """
    Cria dimensao de produto.
    """
    print("\n[INFO] Criando dim_product")
    
    products = sales_df.select(
        "ce_brand_flvr", "brand_nm", "pkg_cat", "pkg_cat_desc", "tsr_pckg_nm"
    ).distinct()
    
    dim_product = (products
        .withColumn("product_key", F.monotonically_increasing_id().cast(IntegerType()) + 1)
        .select(
            "product_key", "ce_brand_flvr", "brand_nm",
            "pkg_cat", "pkg_cat_desc", "tsr_pckg_nm"
        )
    )
    
    # Adiciona auditoria
    dim_product = add_consumption_audit(dim_product, "dim_product")
    
    print(f"[OK] {dim_product.count()} registros criados")
    return dim_product


def create_dim_channel(sales_df: DataFrame, channels_df: DataFrame) -> DataFrame:
    """
    Cria dimensao de canal.
    """
    print("\n[INFO] Criando dim_channel")
    
    # Combina informacoes
    channels_from_sales = sales_df.select(
        "chnl_group", "trade_chnl_desc", "trade_group_desc", "trade_type_desc"
    ).distinct()
    
    channels_enriched = channels_from_sales.join(
        channels_df.select("trade_chnl_desc", "is_alcoholic_channel", "channel_category"),
        on="trade_chnl_desc",
        how="left"
    )
    
    dim_channel = (channels_enriched
        .withColumn("channel_key", F.monotonically_increasing_id().cast(IntegerType()) + 1)
        .select(
            "channel_key", "chnl_group", "trade_chnl_desc",
            "trade_group_desc", "trade_type_desc",
            "is_alcoholic_channel", "channel_category"
        )
    )
    
    # Adiciona auditoria
    dim_channel = add_consumption_audit(dim_channel, "dim_channel")
    
    print(f"[OK] {dim_channel.count()} registros criados")
    return dim_channel


# %%
# Criacao da Tabela Fato

def create_fact_sales(sales_df: DataFrame, dim_date: DataFrame, 
                      dim_region: DataFrame, dim_product: DataFrame,
                      dim_channel: DataFrame) -> DataFrame:
    """
    Cria tabela fato de vendas.
    """
    print("\n[INFO] Criando fact_sales")
    
    # Lookups
    date_lookup = dim_date.select("date_key", "full_date")
    region_lookup = dim_region.select("region_key", "region_name")
    product_lookup = dim_product.select(
        "product_key", "ce_brand_flvr", "brand_nm",
        "pkg_cat", "pkg_cat_desc", "tsr_pckg_nm"
    )
    channel_lookup = dim_channel.select("channel_key", "chnl_group", "trade_chnl_desc")
    
    # Joins
    fact = (sales_df
        # dim_date
        .join(date_lookup,
              sales_df.transaction_date == date_lookup.full_date,
              how="left")
        # dim_region
        .join(region_lookup,
              sales_df.region == region_lookup.region_name,
              how="left")
        # dim_product
        .join(product_lookup,
              (sales_df.ce_brand_flvr == product_lookup.ce_brand_flvr) &
              (sales_df.brand_nm == product_lookup.brand_nm) &
              (sales_df.pkg_cat == product_lookup.pkg_cat) &
              (sales_df.pkg_cat_desc == product_lookup.pkg_cat_desc) &
              (sales_df.tsr_pckg_nm == product_lookup.tsr_pckg_nm),
              how="left")
        # dim_channel
        .join(channel_lookup,
              (sales_df.chnl_group == channel_lookup.chnl_group) &
              (sales_df.trade_chnl_desc == channel_lookup.trade_chnl_desc),
              how="left")
        # Seleciona campos
        .select(
            "date_key", "region_key", "product_key", "channel_key",
            sales_df.dollar_volume.alias("dollar_volume"),
            sales_df.volume_category.alias("volume_category"),
            sales_df.is_negative_sale.alias("is_negative_sale"),
            sales_df.year.alias("year"),
            sales_df.month.alias("month"),
            # Herda campos de auditoria para rastreabilidade
            sales_df._source_file.alias("_source_file"),
            sales_df._ingestion_date.alias("_ingestion_date")
        )
    )
    
    # Adiciona auditoria Consumption
    fact = (fact
        .withColumn("_updated_at", F.current_timestamp())
        .withColumn("_layer", F.lit("consumption"))
        .withColumn("_consumption_timestamp", F.current_timestamp())
        .withColumn("_table_name", F.lit("fact_sales"))
    )
    
    # Valida integridade
    null_keys = fact.filter(
        F.col("date_key").isNull() |
        F.col("region_key").isNull() |
        F.col("product_key").isNull() |
        F.col("channel_key").isNull()
    ).count()
    
    if null_keys > 0:
        print(f"[WARN] {null_keys} registros com chaves nulas")
    else:
        print("[OK] Integridade referencial OK")
    
    print(f"[OK] {fact.count()} registros criados")
    return fact


# %%
# Tabelas Agregadas

def create_agg_top_trade_groups(fact: DataFrame, dim_region: DataFrame,
                                 dim_channel: DataFrame) -> DataFrame:
    """
    Cria agregacao: Top Trade Groups por Regiao.
    Responde pergunta 4.1 do case.
    """
    print("\n[INFO] Criando agg_sales_region_tradegroup")
    
    df = (fact
        .join(dim_region, on="region_key")
        .join(dim_channel, on="channel_key")
    )
    
    agg = (df
        .groupBy("region_name", "trade_group_desc")
        .agg(
            F.sum("dollar_volume").alias("total_dollar_volume"),
            F.count("*").alias("transaction_count")
        )
    )
    
    window = Window.partitionBy("region_name").orderBy(F.desc("total_dollar_volume"))
    
    result = (agg
        .withColumn("rank", F.row_number().over(window))
        .orderBy("region_name", "rank")
    )
    
    # Adiciona auditoria
    result = add_consumption_audit(result, "agg_sales_region_tradegroup")
    
    print(f"[OK] {result.count()} registros criados")
    return result


def create_agg_brand_month(fact: DataFrame, dim_product: DataFrame,
                           dim_date: DataFrame) -> DataFrame:
    """
    Cria agregacao: Vendas por Marca por Mes.
    Responde pergunta 4.2 do case.
    """
    print("\n[INFO] Criando agg_sales_brand_month")
    
    df = (fact
        .join(dim_product, on="product_key")
        .join(dim_date, on="date_key")
    )
    
    result = (df
        .groupBy("brand_nm", "year", "month", "year_month")
        .agg(
            F.sum("dollar_volume").alias("total_dollar_volume"),
            F.count("*").alias("transaction_count")
        )
        .orderBy("brand_nm", "year", "month")
    )
    
    # Adiciona auditoria
    result = add_consumption_audit(result, "agg_sales_brand_month")
    
    print(f"[OK] {result.count()} registros criados")
    return result


def create_agg_brand_region(fact: DataFrame, dim_product: DataFrame,
                            dim_region: DataFrame) -> DataFrame:
    """
    Cria agregacao: Vendas por Marca por Regiao com ranking.
    Responde pergunta 4.3 do case.
    """
    print("\n[INFO] Criando agg_sales_brand_region")
    
    df = (fact
        .join(dim_product, on="product_key")
        .join(dim_region, on="region_key")
    )
    
    agg = (df
        .groupBy("region_name", "brand_nm")
        .agg(
            F.sum("dollar_volume").alias("total_dollar_volume"),
            F.count("*").alias("transaction_count")
        )
    )
    
    window_asc = Window.partitionBy("region_name").orderBy(F.asc("total_dollar_volume"))
    window_desc = Window.partitionBy("region_name").orderBy(F.desc("total_dollar_volume"))
    
    result = (agg
        .withColumn("rank_asc", F.row_number().over(window_asc))
        .withColumn("rank_desc", F.row_number().over(window_desc))
        .orderBy("region_name", "rank_asc")
    )
    
    # Adiciona auditoria
    result = add_consumption_audit(result, "agg_sales_brand_region")
    
    print(f"[OK] {result.count()} registros criados")
    return result


# %%
# Execucao Principal

print("\n" + "=" * 60)
print("CONSUMPTION DIMENSIONAL MODEL")
print("=" * 60)

# Leitura Gold
gold_sales = read_gold("gold_sales_enriched")
gold_channels = read_gold("gold_channels_enriched")

# %%
# Criacao de Dimensoes
dim_date = create_dim_date(gold_sales)
dim_region = create_dim_region(gold_sales)
dim_product = create_dim_product(gold_sales)
dim_channel = create_dim_channel(gold_sales, gold_channels)

# %%
# Visualiza dimensoes
print("\n[INFO] dim_date:")
dim_date.show(5)

print("\n[INFO] dim_region:")
dim_region.show()

print("\n[INFO] dim_product:")
dim_product.show(5)

print("\n[INFO] dim_channel:")
dim_channel.show(5)

# %%
# Criacao da Tabela Fato
fact_sales = create_fact_sales(gold_sales, dim_date, dim_region, dim_product, dim_channel)

print("\n[INFO] fact_sales:")
fact_sales.show(10)

# %%
# Criacao das Agregacoes
agg_trade_groups = create_agg_top_trade_groups(fact_sales, dim_region, dim_channel)
agg_brand_month = create_agg_brand_month(fact_sales, dim_product, dim_date)
agg_brand_region = create_agg_brand_region(fact_sales, dim_product, dim_region)

# %%
# Respostas para as perguntas de negocio

print("\n" + "=" * 60)
print("RESPOSTAS DAS PERGUNTAS DE NEGOCIO")
print("=" * 60)

# Pergunta 4.1: Top 3 Trade Groups por Regiao
print("\n--- PERGUNTA 4.1: Top 3 Trade Groups por Regiao ---")
top3 = agg_trade_groups.filter(F.col("rank") <= 3)
top3.show(30, truncate=False)

# Pergunta 4.2: Vendas por Marca por Mes
print("\n--- PERGUNTA 4.2: Vendas por Marca por Mes ---")
agg_brand_month.show(50)

# Pergunta 4.3: Menor Marca por Regiao
print("\n--- PERGUNTA 4.3: Menor Marca por Regiao ---")
lowest = agg_brand_region.filter(F.col("rank_asc") == 1)
lowest.show(truncate=False)

# %%
# Persistencia

# Dimensoes
save_consumption(dim_date, "dim_date")
save_consumption(dim_region, "dim_region")
save_consumption(dim_product, "dim_product")
save_consumption(dim_channel, "dim_channel")

# Fato
save_consumption(fact_sales, "fact_sales")

# Agregacoes
save_consumption(agg_trade_groups, "agg_sales_region_tradegroup")
save_consumption(agg_brand_month, "agg_sales_brand_month")
save_consumption(agg_brand_region, "agg_sales_brand_region")

# %%
# Resumo Final
print("\n" + "=" * 60)
print("CONSUMPTION DIMENSIONAL MODEL - RESUMO")
print("=" * 60)
print(f"Batch ID: {BATCH_ID}")
print(f"Ambiente: {ENVIRONMENT}")
print(f"\nDimensoes:")
print(f"  dim_date: {dim_date.count()} registros")
print(f"  dim_region: {dim_region.count()} registros")
print(f"  dim_product: {dim_product.count()} registros")
print(f"  dim_channel: {dim_channel.count()} registros")
print(f"\nFato:")
print(f"  fact_sales: {fact_sales.count()} registros")
print(f"\nAgregacoes:")
print(f"  agg_sales_region_tradegroup: {agg_trade_groups.count()} registros")
print(f"  agg_sales_brand_month: {agg_brand_month.count()} registros")
print(f"  agg_sales_brand_region: {agg_brand_region.count()} registros")
print("=" * 60)
print("[OK] Consumption dimensional model concluido com sucesso!")
print("\nPipeline completo: Landing -> Bronze -> Silver -> Gold -> Consumption")

