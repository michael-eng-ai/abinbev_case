"""
Process Control Module

Modulo para registro e monitoramento de execucoes do pipeline.
Registra metadados de cada processamento para observabilidade.
"""

import os
import uuid
from datetime import datetime
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    LongType, DecimalType
)


class ProcessControl:
    """
    Gerencia registros de controle de processos.
    
    Cada execucao de cada tabela em cada camada e registrada
    para rastreabilidade e observabilidade.
    """
    
    SCHEMA = StructType([
        StructField("process_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("start_timestamp", TimestampType(), False),
        StructField("end_timestamp", TimestampType(), True),
        StructField("duration_seconds", DecimalType(10, 2), True),
        StructField("records_read", LongType(), True),
        StructField("records_written", LongType(), True),
        StructField("records_quarantined", LongType(), True),
        StructField("records_failed", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("error_stack_trace", StringType(), True),
        StructField("spark_app_id", StringType(), True),
        StructField("cluster_id", StringType(), True),
        StructField("created_at", TimestampType(), False),
    ])
    
    def __init__(self, spark: SparkSession, control_path: str):
        """
        Inicializa o controlador de processos.
        
        Args:
            spark: SparkSession
            control_path: Caminho base para tabelas de controle
        """
        self.spark = spark
        self.control_path = control_path
        self.table_path = f"{control_path}/process_control"
        self._current_process: Optional[Dict[str, Any]] = None
        
    def start_process(
        self,
        batch_id: str,
        layer: str,
        table_name: str,
    ) -> str:
        """
        Inicia registro de um processo.
        
        Args:
            batch_id: ID do lote de processamento
            layer: Camada (landing, bronze, silver, gold, consumption)
            table_name: Nome da tabela sendo processada
            
        Returns:
            process_id: ID unico do processo
        """
        process_id = str(uuid.uuid4())
        
        self._current_process = {
            "process_id": process_id,
            "batch_id": batch_id,
            "layer": layer,
            "table_name": table_name,
            "status": "RUNNING",
            "start_timestamp": datetime.now(),
            "end_timestamp": None,
            "duration_seconds": None,
            "records_read": None,
            "records_written": None,
            "records_quarantined": 0,
            "records_failed": 0,
            "error_message": None,
            "error_stack_trace": None,
            "spark_app_id": self.spark.sparkContext.applicationId,
            "cluster_id": os.getenv("CLUSTER_ID", "local"),
            "created_at": datetime.now(),
        }
        
        print(f"[PROCESS_CONTROL] Iniciado: {layer}/{table_name} (ID: {process_id})")
        
        return process_id
    
    def end_process(
        self,
        status: str = "SUCCESS",
        records_read: int = 0,
        records_written: int = 0,
        records_quarantined: int = 0,
        records_failed: int = 0,
        error_message: Optional[str] = None,
        error_stack_trace: Optional[str] = None,
    ) -> None:
        """
        Finaliza registro de um processo.
        
        Args:
            status: SUCCESS, FAILED, PARTIAL
            records_read: Registros lidos
            records_written: Registros escritos
            records_quarantined: Registros em quarentena
            records_failed: Registros com erro
            error_message: Mensagem de erro (se houver)
            error_stack_trace: Stack trace (se houver)
        """
        if not self._current_process:
            raise ValueError("Nenhum processo iniciado. Chame start_process primeiro.")
        
        end_time = datetime.now()
        start_time = self._current_process["start_timestamp"]
        duration = (end_time - start_time).total_seconds()
        
        self._current_process.update({
            "status": status,
            "end_timestamp": end_time,
            "duration_seconds": round(duration, 2),
            "records_read": records_read,
            "records_written": records_written,
            "records_quarantined": records_quarantined,
            "records_failed": records_failed,
            "error_message": error_message,
            "error_stack_trace": error_stack_trace,
        })
        
        # Salva registro
        self._save_process_record()
        
        layer = self._current_process["layer"]
        table_name = self._current_process["table_name"]
        
        print(f"[PROCESS_CONTROL] Finalizado: {layer}/{table_name}")
        print(f"  Status: {status}")
        print(f"  Duracao: {duration:.2f}s")
        print(f"  Lidos: {records_read} | Escritos: {records_written}")
        print(f"  Quarentena: {records_quarantined} | Falhas: {records_failed}")
        
        self._current_process = None
    
    def _save_process_record(self) -> None:
        """Salva registro na tabela de controle."""
        if not self._current_process:
            return
        
        # Cria DataFrame com o registro
        record = self._current_process.copy()
        
        # Converte timestamps para strings para criar DataFrame
        record["start_timestamp"] = record["start_timestamp"].isoformat()
        record["end_timestamp"] = record["end_timestamp"].isoformat() if record["end_timestamp"] else None
        record["created_at"] = record["created_at"].isoformat()
        
        df = self.spark.createDataFrame([record])
        
        # Converte de volta para timestamp
        df = (df
            .withColumn("start_timestamp", F.to_timestamp("start_timestamp"))
            .withColumn("end_timestamp", F.to_timestamp("end_timestamp"))
            .withColumn("created_at", F.to_timestamp("created_at"))
        )
        
        # Salva
        try:
            df.write.format("delta").mode("append").save(self.table_path)
        except Exception:
            df.write.mode("append").parquet(self.table_path)
    
    def get_last_successful_batch(self, layer: str, table_name: str) -> Optional[str]:
        """
        Retorna o batch_id da ultima execucao bem sucedida.
        
        Args:
            layer: Camada
            table_name: Nome da tabela
            
        Returns:
            batch_id ou None
        """
        try:
            df = self.spark.read.format("delta").load(self.table_path)
        except Exception:
            try:
                df = self.spark.read.parquet(self.table_path)
            except Exception:
                return None
        
        result = (df
            .filter(
                (F.col("layer") == layer) &
                (F.col("table_name") == table_name) &
                (F.col("status") == "SUCCESS")
            )
            .orderBy(F.col("end_timestamp").desc())
            .select("batch_id")
            .first()
        )
        
        return result.batch_id if result else None
    
    def get_process_history(
        self,
        layer: Optional[str] = None,
        table_name: Optional[str] = None,
        limit: int = 100,
    ) -> DataFrame:
        """
        Retorna historico de execucoes.
        
        Args:
            layer: Filtrar por camada (opcional)
            table_name: Filtrar por tabela (opcional)
            limit: Limite de registros
            
        Returns:
            DataFrame com historico
        """
        try:
            df = self.spark.read.format("delta").load(self.table_path)
        except Exception:
            try:
                df = self.spark.read.parquet(self.table_path)
            except Exception:
                return self.spark.createDataFrame([], self.SCHEMA)
        
        if layer:
            df = df.filter(F.col("layer") == layer)
        
        if table_name:
            df = df.filter(F.col("table_name") == table_name)
        
        return df.orderBy(F.col("start_timestamp").desc()).limit(limit)


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
        
        quarantine_df = (df
            .withColumn("quarantine_id", F.expr("uuid()"))
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
                "quarantine_id", "batch_id", "source_table", "target_table",
                "record_data", "error_type", "error_code", "error_description",
                "dq_rule_name", "is_known_rule", "reprocessed", "reprocess_batch_id",
                "created_at", "updated_at"
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
        
        df = df.filter(F.col("reprocessed") == False)
        
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
        
        return df.filter(
            (F.col("is_known_rule") == False) &
            (F.col("reprocessed") == False)
        )

