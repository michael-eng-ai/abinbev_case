"""
Gold Layer Transformation Functions

Funcoes para enriquecimento de negocio e regras de negocio na camada Gold.
"""

from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_business_metrics(
    df: DataFrame,
    value_column: str,
    group_columns: List[str],
) -> DataFrame:
    """
    Adiciona metricas de negocio agregadas ao DataFrame.

    Args:
        df: DataFrame de entrada
        value_column: Coluna de valor para calcular metricas
        group_columns: Colunas para agrupamento

    Returns:
        DataFrame com metricas adicionadas
    """
    # Calcula metricas por grupo
    metrics_df = df.groupBy(*group_columns).agg(
        F.sum(value_column).alias(f"{value_column}_total"),
        F.avg(value_column).alias(f"{value_column}_avg"),
        F.max(value_column).alias(f"{value_column}_max"),
        F.min(value_column).alias(f"{value_column}_min"),
        F.count("*").alias("record_count"),
    )

    # Join com dados originais
    return df.join(metrics_df, on=group_columns, how="left")


def apply_business_rules(
    df: DataFrame,
    rules: Dict[str, Dict],
) -> DataFrame:
    """
    Aplica regras de negocio para criar campos derivados.

    Args:
        df: DataFrame de entrada
        rules: Dicionario de regras no formato:
            {
                "new_column_name": {
                    "type": "category",
                    "source_column": "value",
                    "conditions": [
                        {"range": [0, 100], "value": "Low"},
                        {"range": [100, 1000], "value": "Medium"},
                        {"range": [1000, None], "value": "High"},
                    ]
                },
                "another_column": {
                    "type": "calculation",
                    "expression": "col_a * col_b / 100"
                }
            }

    Returns:
        DataFrame com colunas de negocio adicionadas
    """
    result = df

    for new_column, rule_config in rules.items():
        rule_type = rule_config.get("type")

        if rule_type == "category":
            source_column = rule_config.get("source_column")
            conditions = rule_config.get("conditions", [])

            # Constroi expressao CASE WHEN
            category_expr = None
            for condition in conditions:
                range_values = condition.get("range", [None, None])
                category_value = condition.get("value")

                low = range_values[0]
                high = range_values[1]

                if low is not None and high is not None:
                    condition_expr = (F.col(source_column) >= low) & (F.col(source_column) < high)
                elif low is not None:
                    condition_expr = F.col(source_column) >= low
                elif high is not None:
                    condition_expr = F.col(source_column) < high
                else:
                    continue

                if category_expr is None:
                    category_expr = F.when(condition_expr, F.lit(category_value))
                else:
                    category_expr = category_expr.when(condition_expr, F.lit(category_value))

            if category_expr is not None:
                result = result.withColumn(new_column, category_expr.otherwise(F.lit("Unknown")))

        elif rule_type == "calculation":
            # Calculos pre-definidos - evita uso de eval por seguranca
            calc_type = rule_config.get("calc_type", "")
            source_cols = rule_config.get("source_columns", [])

            if calc_type == "sum" and len(source_cols) >= 2:
                result = result.withColumn(
                    new_column, F.col(source_cols[0]) + F.col(source_cols[1])
                )
            elif calc_type == "multiply" and len(source_cols) >= 2:
                result = result.withColumn(
                    new_column, F.col(source_cols[0]) * F.col(source_cols[1])
                )
            elif calc_type == "divide" and len(source_cols) >= 2:
                result = result.withColumn(
                    new_column, F.col(source_cols[0]) / F.col(source_cols[1])
                )
            elif calc_type == "subtract" and len(source_cols) >= 2:
                result = result.withColumn(
                    new_column, F.col(source_cols[0]) - F.col(source_cols[1])
                )
            # Adicione mais operacoes conforme necessario

        elif rule_type == "derived":
            source_columns = rule_config.get("source_columns", [])
            operation = rule_config.get("operation", "concat")

            if operation == "concat":
                separator = rule_config.get("separator", "_")
                result = result.withColumn(
                    new_column,
                    F.concat_ws(separator, *[F.col(c) for c in source_columns]),
                )

    return result


def add_time_dimensions(
    df: DataFrame,
    date_column: str,
    prefix: str = "",
) -> DataFrame:
    """
    Adiciona dimensoes temporais derivadas de uma coluna de data.

    Args:
        df: DataFrame de entrada
        date_column: Nome da coluna de data
        prefix: Prefixo para os nomes das novas colunas

    Returns:
        DataFrame com dimensoes temporais
    """
    p = f"{prefix}_" if prefix else ""

    return (
        df.withColumn(f"{p}year", F.year(F.col(date_column)))
        .withColumn(f"{p}month", F.month(F.col(date_column)))
        .withColumn(f"{p}quarter", F.quarter(F.col(date_column)))
    )


def create_fact_table(
    df: DataFrame,
    dimension_keys: List[str],
    measure_columns: List[str],
    aggregations: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """
    Cria uma tabela fato agregada.

    Args:
        df: DataFrame de entrada
        dimension_keys: Lista de colunas que serao chaves para dimensoes
        measure_columns: Lista de colunas de metricas
        aggregations: Dicionario de agregacoes {coluna: tipo}
            Tipos: 'sum', 'avg', 'count', 'max', 'min'

    Returns:
        DataFrame da tabela fato
    """
    if aggregations is None:
        aggregations = {col_name: "sum" for col_name in measure_columns}

    agg_funcs = {
        "sum": F.sum,
        "avg": F.avg,
        "count": F.count,
        "max": F.max,
        "min": F.min,
    }

    agg_expressions = []
    for col_name, agg_type in aggregations.items():
        if col_name in df.columns and agg_type in agg_funcs:
            agg_expressions.append(agg_funcs[agg_type](col_name).alias(f"{col_name}_{agg_type}"))

    return df.groupBy(*dimension_keys).agg(*agg_expressions)


def create_dimension_table(
    df: DataFrame,
    key_column: str,
    attribute_columns: List[str],
    surrogate_key_name: str = "sk",
) -> DataFrame:
    """
    Cria uma tabela dimensao com chave surrogada.

    Args:
        df: DataFrame de entrada
        key_column: Coluna de chave natural
        attribute_columns: Lista de colunas de atributos
        surrogate_key_name: Nome da coluna de chave surrogada

    Returns:
        DataFrame da tabela dimensao
    """
    # Seleciona colunas unicas
    dim_df = df.select([key_column] + attribute_columns).distinct()

    # Adiciona chave surrogada
    dim_df = dim_df.withColumn(surrogate_key_name, F.monotonically_increasing_id() + 1)

    # Adiciona campos de auditoria
    dim_df = (
        dim_df.withColumn("_valid_from", F.current_timestamp())
        .withColumn("_valid_to", F.lit(None).cast("timestamp"))
        .withColumn("_is_current", F.lit(True))
    )

    return dim_df
