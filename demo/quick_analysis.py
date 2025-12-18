#!/usr/bin/env python3
"""
Script de Análise Rápida das Tabelas do Pipeline ABInBev

Este script executa análises rápidas em todas as camadas do pipeline
e gera um relatório em formato texto/markdown.

Uso:
    python quick_analysis.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Setup do path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    """Cria sessão Spark local."""
    return (SparkSession.builder
        .appName("ABInBev_Quick_Analysis")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate())


def analyze_table(spark, path: Path, table_name: str) -> dict:
    """Analisa uma tabela e retorna estatísticas."""
    try:
        df = spark.read.parquet(str(path))
        
        # Estatísticas básicas
        count = df.count()
        columns = len(df.columns)
        
        # Colunas numéricas
        numeric_cols = [f.name for f in df.schema.fields 
                       if str(f.dataType) in ['IntegerType()', 'LongType()', 'DoubleType()', 'DecimalType(10,2)']]
        
        # Estatísticas numéricas
        numeric_stats = {}
        for col in numeric_cols[:3]:  # Limita a 3 colunas
            stats = df.agg(
                F.sum(col).alias("sum"),
                F.avg(col).alias("avg"),
                F.min(col).alias("min"),
                F.max(col).alias("max")
            ).collect()[0]
            numeric_stats[col] = {
                "sum": stats["sum"],
                "avg": stats["avg"],
                "min": stats["min"],
                "max": stats["max"]
            }
        
        return {
            "table_name": table_name,
            "record_count": count,
            "column_count": columns,
            "columns": df.columns,
            "numeric_stats": numeric_stats,
            "status": "OK"
        }
    except Exception as e:
        return {
            "table_name": table_name,
            "status": "ERROR",
            "error": str(e)
        }


def print_separator(char="=", length=70):
    """Imprime separador."""
    print(char * length)


def main():
    """Função principal."""
    print_separator()
    print("        ABInBev Beverage Analytics - Análise Rápida")
    print_separator()
    print(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Projeto: {PROJECT_ROOT}")
    print()
    
    # Inicializa Spark
    print("Iniciando Spark Session...")
    spark = create_spark_session()
    print(f"Spark Version: {spark.version}")
    print()
    
    # Define paths
    data_path = PROJECT_ROOT / "data"
    
    layers = {
        "BRONZE": {
            "beverage_sales": data_path / "bronze" / "beverage_sales",
            "channel_features": data_path / "bronze" / "channel_features"
        },
        "SILVER": {
            "beverage_sales": data_path / "silver" / "beverage_sales",
            "channel_features": data_path / "silver" / "channel_features"
        },
        "GOLD": {
            "beverage_sales_enriched": data_path / "gold" / "beverage_sales_enriched"
        },
        "CONSUMPTION": {
            "dim_time": data_path / "consumption" / "dim_time",
            "dim_product": data_path / "consumption" / "dim_product",
            "dim_region": data_path / "consumption" / "dim_region",
            "dim_channel": data_path / "consumption" / "dim_channel",
            "fact_sales": data_path / "consumption" / "fact_sales",
            "agg_sales_by_month": data_path / "consumption" / "agg_sales_by_month",
            "agg_sales_by_trade_group": data_path / "consumption" / "agg_sales_by_trade_group",
            "agg_lowest_brand_by_region": data_path / "consumption" / "agg_lowest_brand_by_region"
        }
    }
    
    # Analisa cada camada
    all_results = {}
    
    for layer_name, tables in layers.items():
        print_separator("-")
        print(f"Camada: {layer_name}")
        print_separator("-")
        
        layer_results = {}
        for table_name, table_path in tables.items():
            result = analyze_table(spark, table_path, table_name)
            layer_results[table_name] = result
            
            if result["status"] == "OK":
                print(f"  [OK] {table_name}")
                print(f"     • Registros: {result['record_count']:,}")
                print(f"     • Colunas: {result['column_count']}")
                
                if result.get("numeric_stats"):
                    for col, stats in result["numeric_stats"].items():
                        if stats["sum"] is not None:
                            print(f"     • {col}: sum={stats['sum']:,.2f}, avg={stats['avg']:,.2f}")
            else:
                print(f"  [ERROR] {table_name}: {result.get('error', 'Unknown error')}")
            print()
        
        all_results[layer_name] = layer_results
    
    # Sumário
    print_separator()
    print("SUMÁRIO DO PIPELINE")
    print_separator()
    
    total_tables = 0
    total_records = 0
    successful_tables = 0
    
    for layer_name, tables in all_results.items():
        for table_name, result in tables.items():
            total_tables += 1
            if result["status"] == "OK":
                successful_tables += 1
                total_records += result.get("record_count", 0)
    
    print(f"  • Total de Tabelas: {total_tables}")
    print(f"  • Tabelas OK: {successful_tables}")
    print(f"  • Tabelas com Erro: {total_tables - successful_tables}")
    print(f"  • Total de Registros: {total_records:,}")
    print()
    
    # Business Queries
    print_separator()
    print("BUSINESS QUERIES")
    print_separator()
    
    # Query 1: Top 3 Trade Groups
    try:
        agg_trade = spark.read.parquet(str(data_path / "consumption" / "agg_sales_by_trade_group"))
        top3 = agg_trade.orderBy(F.desc("total_volume")).limit(3).collect()
        
        print("\nTop 3 Trade Groups por Volume:")
        for i, row in enumerate(top3, 1):
            print(f"   {i}. {row['trade_group_desc']}: {row['total_volume']:,.0f} HL")
    except Exception as e:
        print(f"  [ERROR] Erro na Query 1: {e}")
    
    # Query 2: Vendas por Mês
    try:
        agg_month = spark.read.parquet(str(data_path / "consumption" / "agg_sales_by_month"))
        months = agg_month.orderBy("year", "month").collect()
        
        print("\nVendas por Mês:")
        for row in months[:6]:  # Primeiros 6 meses
            print(f"   • {row['year']}/{row['month']:02d}: {row['total_volume']:,.0f} HL")
        if len(months) > 6:
            print(f"   ... e mais {len(months) - 6} meses")
    except Exception as e:
        print(f"  [ERROR] Erro na Query 2: {e}")
    
    # Query 3: Menor Marca por Região
    try:
        agg_lowest = spark.read.parquet(str(data_path / "consumption" / "agg_lowest_brand_by_region"))
        lowest = agg_lowest.collect()
        
        print("\nMarca com Menor Volume por Região:")
        for row in lowest[:5]:  # Primeiras 5 regiões
            print(f"   • {row['region']}: {row['lowest_selling_brand']} ({row['total_volume']:,.0f} HL)")
        if len(lowest) > 5:
            print(f"   ... e mais {len(lowest) - 5} regiões")
    except Exception as e:
        print(f"  [ERROR] Erro na Query 3: {e}")
    
    print()
    print_separator()
    print("Análise concluída!")
    print_separator()
    
    # Encerra Spark
    spark.stop()


if __name__ == "__main__":
    main()
