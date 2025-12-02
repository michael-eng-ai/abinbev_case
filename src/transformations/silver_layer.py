"""
Silver Layer Transformation Functions

Funcoes para limpeza, validacao e qualidade de dados na camada Silver.
"""

from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    regexp_replace,
    trim,
    when,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def clean_string_columns(df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
    """
    Limpa colunas de texto removendo espacos extras e caracteres especiais.

    Args:
        df: DataFrame de entrada
        columns: Lista de colunas a limpar (None = todas as colunas string)

    Returns:
        DataFrame com colunas limpas
    """
    if columns is None:
        columns = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]

    result = df
    for column in columns:
        if column in df.columns:
            result = result.withColumn(column, trim(col(column)))

    return result


def cast_columns(df: DataFrame, column_types: Dict[str, str]) -> DataFrame:
    """
    Converte tipos de colunas conforme especificado.

    Args:
        df: DataFrame de entrada
        column_types: Dicionario {nome_coluna: tipo_destino}
            Tipos suportados: 'int', 'long', 'double', 'string', 'date', 'timestamp'

    Returns:
        DataFrame com colunas convertidas
    """
    type_mapping = {
        "int": IntegerType(),
        "integer": IntegerType(),
        "long": LongType(),
        "bigint": LongType(),
        "double": DoubleType(),
        "float": DoubleType(),
        "string": StringType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }

    result = df
    for column, dtype in column_types.items():
        if column in df.columns:
            target_type = type_mapping.get(dtype.lower())
            if target_type:
                result = result.withColumn(column, col(column).cast(target_type))

    return result


def apply_data_quality_rules(
    df: DataFrame,
    rules: List[Dict],
) -> Tuple[DataFrame, DataFrame]:
    """
    Aplica regras de qualidade de dados e separa registros validos e invalidos.

    Args:
        df: DataFrame de entrada
        rules: Lista de regras no formato:
            [
                {"column": "col_name", "rule": "not_null"},
                {"column": "col_name", "rule": "positive"},
                {"column": "col_name", "rule": "in_list", "values": [1, 2, 3]},
                {"column": "col_name", "rule": "regex", "pattern": "^[A-Z]+$"},
            ]

    Returns:
        Tupla (DataFrame valido, DataFrame quarentena)
    """
    # Adiciona coluna de validacao
    result = df.withColumn("_is_valid", lit(True))
    result = result.withColumn("_validation_errors", lit(""))

    for rule in rules:
        column = rule.get("column")
        rule_type = rule.get("rule")

        if column not in df.columns:
            continue

        if rule_type == "not_null":
            result = result.withColumn(
                "_is_valid",
                when(col(column).isNull(), lit(False)).otherwise(col("_is_valid")),
            )
            result = result.withColumn(
                "_validation_errors",
                when(
                    col(column).isNull(),
                    concat_with_separator(col("_validation_errors"), f"{column}:null"),
                ).otherwise(col("_validation_errors")),
            )

        elif rule_type == "positive":
            result = result.withColumn(
                "_is_valid",
                when(col(column) <= 0, lit(False)).otherwise(col("_is_valid")),
            )
            result = result.withColumn(
                "_validation_errors",
                when(
                    col(column) <= 0,
                    concat_with_separator(col("_validation_errors"), f"{column}:not_positive"),
                ).otherwise(col("_validation_errors")),
            )

        elif rule_type == "in_list":
            allowed_values = rule.get("values", [])
            result = result.withColumn(
                "_is_valid",
                when(~col(column).isin(allowed_values), lit(False)).otherwise(col("_is_valid")),
            )

        elif rule_type == "regex":
            pattern = rule.get("pattern", ".*")
            result = result.withColumn(
                "_is_valid",
                when(
                    ~col(column).rlike(pattern),
                    lit(False),
                ).otherwise(col("_is_valid")),
            )

    # Separa registros validos e invalidos
    valid_df = result.filter(col("_is_valid")).drop("_is_valid", "_validation_errors")
    quarantine_df = result.filter(~col("_is_valid")).drop("_is_valid")

    # Adiciona timestamp de quarentena
    quarantine_df = quarantine_df.withColumn("_quarantine_timestamp", current_timestamp())

    return valid_df, quarantine_df


def concat_with_separator(existing_col, new_value: str):
    """Helper para concatenar erros de validacao."""
    from pyspark.sql.functions import concat, lit, when, length

    return when(
        length(existing_col) > 0,
        concat(existing_col, lit("; "), lit(new_value)),
    ).otherwise(lit(new_value))


def merge_with_existing(
    new_df: DataFrame,
    existing_df: DataFrame,
    pk_column: str = "_pk",
    hash_column: str = "_row_hash",
) -> DataFrame:
    """
    Realiza merge incremental (UPSERT) baseado em PK e hash de linha.

    Mantem o registro mais recente quando ha mudancas.

    Args:
        new_df: DataFrame com novos dados
        existing_df: DataFrame com dados existentes
        pk_column: Nome da coluna de chave primaria
        hash_column: Nome da coluna de hash de linha

    Returns:
        DataFrame merged com dados atualizados
    """
    # Registros novos (nao existem no destino)
    new_records = new_df.join(
        existing_df.select(pk_column),
        on=pk_column,
        how="left_anti",
    )

    # Registros atualizados (existem mas hash diferente)
    updated_records = new_df.alias("new").join(
        existing_df.alias("existing").select(pk_column, hash_column),
        on=pk_column,
        how="inner",
    ).filter(
        col(f"new.{hash_column}") != col(f"existing.{hash_column}")
    ).select("new.*")

    # Registros inalterados (existem e hash igual)
    unchanged_records = existing_df.alias("existing").join(
        new_df.alias("new").select(pk_column, hash_column),
        on=pk_column,
        how="inner",
    ).filter(
        col(f"new.{hash_column}") == col(f"existing.{hash_column}")
    ).select("existing.*")

    # Registros que nao vieram no novo batch (manter)
    not_in_batch = existing_df.join(
        new_df.select(pk_column),
        on=pk_column,
        how="left_anti",
    )

    # Combina todos
    return (
        new_records
        .unionByName(updated_records)
        .unionByName(unchanged_records)
        .unionByName(not_in_batch)
    )

