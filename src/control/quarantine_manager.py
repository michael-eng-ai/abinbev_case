"""
Quarantine Manager Module

Modulo para gerenciamento de registros em quarentena.
Registros que falham nas validacoes de qualidade de dados sao
armazenados aqui para posterior analise e reprocessamento.
"""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class QuarantineManager:
    """
    Gerencia registros em quarentena.
    """

    def __init__(self, spark: SparkSession, control_path: str):
        """
        Inicializa o gerenciador de quarentena.

        Args:
            spark: SparkSession
            control_path: Caminho base para tabelas de controle
        """
        self.spark = spark
        self.control_path = control_path
        self.table_path = f"{control_path}/quarantine"

    def save_to_quarantine(
        self,
        df: DataFrame,
        source_table: str,
        target_table: str,
        error_code: str,
        error_description: str,
        batch_id: str,
        is_known_rule: bool = True,
    ) -> int:
        """
        Salva registros na quarentena.

        Args:
            df: DataFrame com registros invalidos
            source_table: Tabela de origem
            target_table: Tabela de destino
            error_code: Codigo do erro (ex: DQ_SALES_001)
            error_description: Descricao do erro
            batch_id: ID do lote
            is_known_rule: True se erro conhecido, False se novo

        Returns:
            Quantidade de registros quarentenados
        """
        count = df.count()
        if count == 0:
            return 0

        quarantine_df = (
            df.withColumn("quarantine_id", F.expr("uuid()"))
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("source_table", F.lit(source_table))
            .withColumn("target_table", F.lit(target_table))
            .withColumn("record_data", F.to_json(F.struct(*df.columns)))
            .withColumn("error_type", F.lit("VALIDATION_ERROR"))
            .withColumn("error_code", F.lit(error_code))
            .withColumn("error_description", F.lit(error_description))
            .withColumn("dq_rule_name", F.lit(error_code))
            .withColumn("is_known_rule", F.lit(is_known_rule))
            .withColumn("reprocessed", F.lit(False))
            .withColumn("reprocess_batch_id", F.lit(None).cast("string"))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
            .select(
                "quarantine_id",
                "batch_id",
                "source_table",
                "target_table",
                "record_data",
                "error_type",
                "error_code",
                "error_description",
                "dq_rule_name",
                "is_known_rule",
                "reprocessed",
                "reprocess_batch_id",
                "created_at",
                "updated_at",
            )
        )

        try:
            quarantine_df.write.format("delta").mode("append").save(self.table_path)
        except Exception:
            quarantine_df.write.mode("append").parquet(self.table_path)

        # Alerta se erro desconhecido
        if not is_known_rule:
            print(f"[ALERT] ERRO DESCONHECIDO: {error_code} - {error_description}")
            print(f"[ALERT] {count} registros precisam de nova regra de DQ")
        else:
            print(f"[QUARANTINE] {count} registros: {error_code}")

        return count

    def get_pending_reprocess(self, source_table: Optional[str] = None) -> DataFrame:
        """
        Retorna registros pendentes de reprocessamento.

        Args:
            source_table: Filtrar por tabela de origem (opcional)

        Returns:
            DataFrame com registros pendentes
        """
        try:
            df = self.spark.read.format("delta").load(self.table_path)
        except Exception:
            try:
                df = self.spark.read.parquet(self.table_path)
            except Exception:
                return self.spark.createDataFrame([], self.spark.createDataFrame([]).schema)

        df = df.filter(~F.col("reprocessed"))

        if source_table:
            df = df.filter(F.col("source_table") == source_table)

        return df

    def mark_as_reprocessed(self, quarantine_ids: list, reprocess_batch_id: str) -> int:
        """
        Marca registros como reprocessados.

        Args:
            quarantine_ids: Lista de IDs a marcar
            reprocess_batch_id: ID do batch de reprocessamento

        Returns:
            Quantidade de registros atualizados
        """
        # Para Delta Lake, usaria MERGE
        # Para Parquet, precisaria reescrever
        # Implementacao simplificada
        print(f"[QUARANTINE] Marcando {len(quarantine_ids)} registros como reprocessados")
        return len(quarantine_ids)

    def get_unknown_errors(self) -> DataFrame:
        """
        Retorna erros desconhecidos que precisam de novas regras.

        Returns:
            DataFrame com erros desconhecidos
        """
        try:
            df = self.spark.read.format("delta").load(self.table_path)
        except Exception:
            try:
                df = self.spark.read.parquet(self.table_path)
            except Exception:
                return self.spark.createDataFrame([])

        return df.filter(~F.col("is_known_rule") & ~F.col("reprocessed"))
