"""
Bronze Layer Transformation Functions

Funcoes para adicionar colunas de auditoria, chaves primarias e hashes
para a camada Bronze da arquitetura Medallion.
"""

import re
from datetime import datetime
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Padroniza os nomes das colunas para snake_case e lowercase.

    Args:
        df: DataFrame de entrada

    Returns:
        DataFrame com nomes de colunas padronizados
    """
    new_columns = []
    for column in df.columns:
        # Remove espacos extras e converte para lowercase
        new_name = column.strip().lower()
        # Substitui espacos e caracteres especiais por underscore
        new_name = re.sub(r"[^a-z0-9]+", "_", new_name)
        # Remove underscores duplicados
        new_name = re.sub(r"_+", "_", new_name)
        # Remove underscores no inicio e fim
        new_name = new_name.strip("_")
        new_columns.append(new_name)

    return df.toDF(*new_columns)


def add_audit_columns(
    df: DataFrame,
    source_file: str,
    source_path: str,
    layer: str,
    batch_id: Optional[str] = None,
) -> DataFrame:
    """
    Adiciona colunas de auditoria padrao ao DataFrame.

    Args:
        df: DataFrame de entrada
        source_file: Nome do arquivo de origem
        source_path: Caminho completo do arquivo de origem
        layer: Nome da camada (bronze, silver, gold, consumption)
        batch_id: Identificador do batch (opcional)

    Returns:
        DataFrame com colunas de auditoria
    """
    now = datetime.now()
    batch = batch_id or now.strftime("%Y%m%d%H%M%S")

    return (
        df.withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_path", F.lit(source_path))
        .withColumn("_ingestion_date", F.lit(now.strftime("%Y-%m-%d")))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch))
        .withColumn("_updated_at", F.current_timestamp())
        .withColumn("_layer", F.lit(layer))
    )


def add_primary_key(df: DataFrame, key_columns: List[str]) -> DataFrame:
    """
    Adiciona uma coluna de chave primaria (_pk) baseada no hash MD5
    das colunas especificadas.

    Args:
        df: DataFrame de entrada
        key_columns: Lista de nomes de colunas para compor a chave primaria

    Returns:
        DataFrame com coluna _pk adicionada
    """
    # Verifica se todas as colunas existem
    missing_cols = [c for c in key_columns if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Colunas nao encontradas: {missing_cols}")

    return df.withColumn(
        "_pk",
        F.md5(F.concat_ws("||", *[F.trim(F.col(c).cast("string")) for c in key_columns])),
    )


def add_row_hash(
    df: DataFrame,
    exclude_columns: Optional[List[str]] = None,
) -> DataFrame:
    """
    Adiciona uma coluna de hash de linha (_row_hash) baseada no SHA256
    de todas as colunas exceto as especificadas.

    Usado para deteccao de mudancas (CDC) entre processamentos.

    Args:
        df: DataFrame de entrada
        exclude_columns: Lista de colunas a excluir do hash

    Returns:
        DataFrame com coluna _row_hash adicionada
    """
    if exclude_columns is None:
        exclude_columns = [
            "_pk",
            "_row_hash",
            "_source_file",
            "_source_path",
            "_ingestion_date",
            "_ingestion_timestamp",
            "_batch_id",
            "_updated_at",
            "_layer",
        ]

    hash_columns = [c for c in df.columns if c not in exclude_columns]

    return df.withColumn(
        "_row_hash",
        F.sha2(F.concat_ws("||", *[F.trim(F.col(c).cast("string")) for c in hash_columns]), 256),
    )


def update_layer_timestamp(df: DataFrame, layer: str) -> DataFrame:
    """
    Atualiza os campos _updated_at e _layer para a camada atual.

    Args:
        df: DataFrame de entrada
        layer: Nome da camada atual

    Returns:
        DataFrame com campos atualizados
    """
    return (
        df.withColumn("_updated_at", F.current_timestamp())
        .withColumn("_layer", F.lit(layer))
        .withColumn(f"_{layer}_timestamp", F.current_timestamp())
    )
