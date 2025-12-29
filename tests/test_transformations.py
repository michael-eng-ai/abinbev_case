"""
Unit Tests for Transformation Functions

Testes unitarios para as funcoes de transformacao de cada camada.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.transformations.bronze_layer import (
    add_audit_columns,
    add_primary_key,
    add_row_hash,
    standardize_column_names,
)
from src.transformations.gold_layer import (
    add_business_metrics,
    create_dimension_table,
    create_fact_table,
)
from src.transformations.silver_layer import (
    apply_data_quality_rules,
    cast_columns,
    clean_string_columns,
)


class TestBronzeLayer:
    """Testes para funcoes da camada Bronze."""

    def test_standardize_column_names(self, spark):
        """Testa padronizacao de nomes de colunas."""
        data = [("a", 1), ("b", 2)]
        df = spark.createDataFrame(data, ["Column Name", "Another COLUMN"])

        result = standardize_column_names(df)

        assert "column_name" in result.columns
        assert "another_column" in result.columns
        assert len(result.columns) == 2

    def test_add_audit_columns(self, spark):
        """Testa adicao de colunas de auditoria."""
        data = [("a", 1), ("b", 2)]
        df = spark.createDataFrame(data, ["col1", "col2"])

        result = add_audit_columns(
            df,
            source_file="test.csv",
            source_path="/data/test.csv",
            layer="bronze",
            batch_id="20240101120000",
        )

        assert "_source_file" in result.columns
        assert "_source_path" in result.columns
        assert "_ingestion_date" in result.columns
        assert "_ingestion_timestamp" in result.columns
        assert "_batch_id" in result.columns
        assert "_updated_at" in result.columns
        assert "_layer" in result.columns

        row = result.first()
        assert row._source_file == "test.csv"
        assert row._layer == "bronze"
        assert row._batch_id == "20240101120000"

    def test_add_primary_key(self, spark):
        """Testa adicao de chave primaria."""
        data = [("a", 1), ("b", 2), ("a", 1)]
        df = spark.createDataFrame(data, ["col1", "col2"])

        result = add_primary_key(df, ["col1", "col2"])

        assert "_pk" in result.columns
        # Linhas com mesmos valores devem ter mesmo PK
        pks = result.select("_pk").collect()
        assert pks[0]._pk == pks[2]._pk
        assert pks[0]._pk != pks[1]._pk

    def test_add_primary_key_missing_column(self, spark):
        """Testa erro quando coluna nao existe."""
        data = [("a", 1)]
        df = spark.createDataFrame(data, ["col1", "col2"])

        with pytest.raises(ValueError, match="Colunas nao encontradas"):
            add_primary_key(df, ["col1", "col_missing"])

    def test_add_row_hash(self, spark):
        """Testa adicao de hash de linha."""
        data = [("a", 1), ("b", 2), ("a", 1)]
        df = spark.createDataFrame(data, ["col1", "col2"])

        result = add_row_hash(df)

        assert "_row_hash" in result.columns
        # Linhas identicas devem ter mesmo hash
        hashes = result.select("_row_hash").collect()
        assert hashes[0]._row_hash == hashes[2]._row_hash
        assert hashes[0]._row_hash != hashes[1]._row_hash


class TestSilverLayer:
    """Testes para funcoes da camada Silver."""

    def test_clean_string_columns(self, spark):
        """Testa limpeza de colunas de texto."""
        data = [("  hello  ", 1), ("world  ", 2)]
        df = spark.createDataFrame(data, ["text", "num"])

        result = clean_string_columns(df, ["text"])

        values = [row.text for row in result.collect()]
        assert values == ["hello", "world"]

    def test_cast_columns(self, spark):
        """Testa conversao de tipos."""
        data = [("100", "2024-01-01"), ("200", "2024-02-01")]
        df = spark.createDataFrame(data, ["value", "date"])

        result = cast_columns(df, {"value": "int", "date": "date"})

        # Verifica tipos
        from pyspark.sql.types import DateType, IntegerType

        assert isinstance(result.schema["value"].dataType, IntegerType)
        assert isinstance(result.schema["date"].dataType, DateType)

    def test_apply_data_quality_rules_not_null(self, spark):
        """Testa regra de not null."""
        data = [("a", 1), (None, 2), ("c", None)]
        df = spark.createDataFrame(data, ["col1", "col2"])

        rules = [
            {"column": "col1", "rule": "not_null"},
            {"column": "col2", "rule": "not_null"},
        ]

        valid_df, quarantine_df = apply_data_quality_rules(df, rules)

        assert valid_df.count() == 1
        assert quarantine_df.count() == 2

    def test_apply_data_quality_rules_positive(self, spark):
        """Testa regra de valor positivo."""
        data = [("a", 100), ("b", -50), ("c", 0)]
        df = spark.createDataFrame(data, ["name", "value"])

        rules = [{"column": "value", "rule": "positive"}]

        valid_df, quarantine_df = apply_data_quality_rules(df, rules)

        assert valid_df.count() == 1
        assert quarantine_df.count() == 2


class TestGoldLayer:
    """Testes para funcoes da camada Gold."""

    def test_add_business_metrics(self, sample_sales_data):
        """Testa adicao de metricas de negocio."""
        result = add_business_metrics(
            sample_sales_data,
            value_column="hl_qty",
            group_columns=["btlr_org_lvl_c"],
        )

        assert "hl_qty_total" in result.columns
        assert "hl_qty_avg" in result.columns
        assert "record_count" in result.columns

    def test_create_fact_table(self, sample_sales_data):
        """Testa criacao de tabela fato."""
        result = create_fact_table(
            sample_sales_data,
            dimension_keys=["btlr_org_lvl_c", "month_id"],
            measure_columns=["hl_qty", "transaction_count"],
        )

        assert "hl_qty_sum" in result.columns
        assert "transaction_count_sum" in result.columns
        # Deveria ter 2 combinacoes unicas de regiao + mes
        # REG1+202101 e REG2+202102
        assert result.count() == 2

    def test_create_dimension_table(self, sample_sales_data):
        """Testa criacao de tabela dimensao."""
        result = create_dimension_table(
            sample_sales_data,
            key_column="brand_nm",
            attribute_columns=[],
            surrogate_key_name="brand_sk",
        )

        assert "brand_sk" in result.columns
        assert "_valid_from" in result.columns
        assert "_is_current" in result.columns
        # Deveria ter 3 marcas unicas
        assert result.count() == 3
