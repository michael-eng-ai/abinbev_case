"""
ABInBev Case - Transformation Functions Module

Este modulo contem funcoes reutilizaveis para transformacoes de dados
em cada camada da arquitetura Medallion.

NOTA: Este modulo e COMPLEMENTAR aos notebooks existentes.
Os notebooks (01_bronze_ingestion.py, 02_silver_transformation.py, etc.)
contem a logica completa de:
- CDC (PK + row_hash)
- Merge incremental
- Tabela de quarentena
- Regras de Data Quality

As funcoes aqui podem ser importadas para testes ou reutilizacao.
"""

from src.transformations.bronze_layer import (
    add_audit_columns,
    add_primary_key,
    add_row_hash,
    standardize_column_names,
    update_layer_timestamp,
)
from src.transformations.silver_layer import (
    apply_data_quality_rules,
    clean_string_columns,
    cast_columns,
    merge_with_existing,
)
from src.transformations.gold_layer import (
    add_business_metrics,
    apply_business_rules,
    add_time_dimensions,
    create_fact_table,
    create_dimension_table,
)

__all__ = [
    # Bronze
    "add_audit_columns",
    "add_primary_key",
    "add_row_hash",
    "standardize_column_names",
    "update_layer_timestamp",
    # Silver
    "apply_data_quality_rules",
    "clean_string_columns",
    "cast_columns",
    "merge_with_existing",
    # Gold
    "add_business_metrics",
    "apply_business_rules",
    "add_time_dimensions",
    "create_fact_table",
    "create_dimension_table",
]

