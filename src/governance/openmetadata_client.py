# ==============================================================================
# ABInBev Case - OpenMetadata Client
# ==============================================================================
#
# Este modulo fornece integracao com OpenMetadata para:
# - Registro de tabelas no catalogo
# - Registro de lineage
# - Registro de qualidade de dados
# - Atualizacao de metadata
#
# ==============================================================================

import os
import json
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict


@dataclass
class TableMetadata:
    """Metadata de uma tabela."""
    name: str
    layer: str  # bronze, silver, gold, consumption
    database: str
    description: str
    columns: List[Dict[str, str]]
    tags: List[str]
    owner: Optional[str] = None


@dataclass
class LineageEdge:
    """Representa uma aresta de lineage."""
    source_table: str
    target_table: str
    transformation: str
    columns_mapping: Optional[Dict[str, str]] = None


@dataclass
class DataQualityResult:
    """Resultado de teste de qualidade."""
    table_name: str
    test_name: str
    test_type: str
    passed: bool
    value: float
    threshold: float
    timestamp: str


class OpenMetadataClient:
    """
    Cliente para integracao com OpenMetadata.
    
    Uso:
        client = OpenMetadataClient()
        client.register_table(table_metadata)
        client.register_lineage(lineage_edge)
    """
    
    def __init__(self):
        """Inicializa o cliente."""
        self.base_url = os.getenv("OPENMETADATA_URL", "http://localhost:8585/api/v1")
        self.auth_token = os.getenv("OPENMETADATA_TOKEN", "")
        self.enabled = os.getenv("GOVERNANCE_ENABLED", "true").lower() == "true"
        
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        if self.auth_token:
            self.headers["Authorization"] = f"Bearer {self.auth_token}"
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """Faz requisicao para a API."""
        if not self.enabled:
            return {"status": "disabled", "message": "OpenMetadata disabled"}
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            if method == "GET":
                response = requests.get(url, headers=self.headers)
            elif method == "POST":
                response = requests.post(url, headers=self.headers, json=data)
            elif method == "PUT":
                response = requests.put(url, headers=self.headers, json=data)
            elif method == "PATCH":
                response = requests.patch(url, headers=self.headers, json=data)
            else:
                return {"error": f"Method {method} not supported"}
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.ConnectionError:
            return {"error": "Connection failed", "message": "OpenMetadata not available"}
        except requests.exceptions.HTTPError as e:
            return {"error": str(e)}
        except Exception as e:
            return {"error": str(e)}
    
    def register_table(self, metadata: TableMetadata) -> Dict:
        """
        Registra uma tabela no catalogo.
        
        Args:
            metadata: Metadata da tabela
            
        Returns:
            Resposta da API
        """
        # Constroi o payload
        columns = [
            {
                "name": col["name"],
                "dataType": col.get("type", "STRING"),
                "description": col.get("description", ""),
            }
            for col in metadata.columns
        ]
        
        payload = {
            "name": metadata.name,
            "displayName": metadata.name,
            "description": metadata.description,
            "tableType": "Regular",
            "columns": columns,
            "database": f"{metadata.database}.{metadata.layer}",
            "tags": [{"tagFQN": f"Tier.{metadata.layer.capitalize()}"} for tag in metadata.tags],
        }
        
        if metadata.owner:
            payload["owner"] = {"name": metadata.owner}
        
        print(f"[GOVERNANCE] Registrando tabela: {metadata.layer}.{metadata.name}")
        return self._make_request("POST", "tables", payload)
    
    def register_lineage(self, edge: LineageEdge) -> Dict:
        """
        Registra lineage entre tabelas.
        
        Args:
            edge: Aresta de lineage
            
        Returns:
            Resposta da API
        """
        payload = {
            "edge": {
                "fromEntity": {
                    "type": "table",
                    "fqn": edge.source_table
                },
                "toEntity": {
                    "type": "table",
                    "fqn": edge.target_table
                },
                "lineageDetails": {
                    "description": edge.transformation,
                    "columnsLineage": edge.columns_mapping or []
                }
            }
        }
        
        print(f"[GOVERNANCE] Registrando lineage: {edge.source_table} -> {edge.target_table}")
        return self._make_request("PUT", "lineage", payload)
    
    def register_data_quality(self, result: DataQualityResult) -> Dict:
        """
        Registra resultado de teste de qualidade.
        
        Args:
            result: Resultado do teste
            
        Returns:
            Resposta da API
        """
        payload = {
            "testCaseResult": {
                "timestamp": result.timestamp,
                "testCaseStatus": "Success" if result.passed else "Failed",
                "result": f"{result.value}",
                "testResultValue": [
                    {
                        "name": result.test_name,
                        "value": str(result.value)
                    }
                ]
            }
        }
        
        print(f"[GOVERNANCE] Registrando DQ: {result.table_name}.{result.test_name} = {'PASSED' if result.passed else 'FAILED'}")
        return self._make_request("PUT", f"dataQuality/testCases/{result.table_name}/{result.test_name}/testCaseResult", payload)
    
    def update_table_description(self, table_fqn: str, description: str) -> Dict:
        """Atualiza descricao de uma tabela."""
        payload = [
            {
                "op": "add",
                "path": "/description",
                "value": description
            }
        ]
        
        return self._make_request("PATCH", f"tables/name/{table_fqn}", payload)
    
    def add_tag(self, table_fqn: str, tag: str) -> Dict:
        """Adiciona tag a uma tabela."""
        payload = [
            {
                "op": "add",
                "path": "/tags/0",
                "value": {"tagFQN": tag}
            }
        ]
        
        return self._make_request("PATCH", f"tables/name/{table_fqn}", payload)


class LineageTracker:
    """
    Helper para rastrear lineage durante o processamento.
    
    Uso:
        tracker = LineageTracker("bronze", "silver")
        tracker.add_table_transformation("bronze_sales", "silver_sales", "DQ + Limpeza")
        tracker.commit()
    """
    
    def __init__(self, source_layer: str, target_layer: str):
        """Inicializa o tracker."""
        self.source_layer = source_layer
        self.target_layer = target_layer
        self.edges: List[LineageEdge] = []
        self.client = OpenMetadataClient()
    
    def add_table_transformation(self, source_table: str, target_table: str, 
                                  transformation: str, columns_mapping: Optional[Dict] = None):
        """Adiciona uma transformacao ao tracker."""
        edge = LineageEdge(
            source_table=f"abinbev.{self.source_layer}.{source_table}",
            target_table=f"abinbev.{self.target_layer}.{target_table}",
            transformation=transformation,
            columns_mapping=columns_mapping
        )
        self.edges.append(edge)
    
    def commit(self):
        """Envia todas as arestas de lineage para o OpenMetadata."""
        for edge in self.edges:
            self.client.register_lineage(edge)
        
        print(f"[GOVERNANCE] {len(self.edges)} edges de lineage registrados")


class DataQualityReporter:
    """
    Reporter de qualidade de dados para OpenMetadata.
    
    Uso:
        reporter = DataQualityReporter("silver_sales")
        reporter.report_check("null_check", "completeness", True, 0.99, 0.95)
        reporter.commit()
    """
    
    def __init__(self, table_name: str):
        """Inicializa o reporter."""
        self.table_name = table_name
        self.results: List[DataQualityResult] = []
        self.client = OpenMetadataClient()
    
    def report_check(self, test_name: str, test_type: str, passed: bool, 
                     value: float, threshold: float):
        """Adiciona resultado de teste."""
        result = DataQualityResult(
            table_name=self.table_name,
            test_name=test_name,
            test_type=test_type,
            passed=passed,
            value=value,
            threshold=threshold,
            timestamp=datetime.utcnow().isoformat()
        )
        self.results.append(result)
    
    def commit(self):
        """Envia todos os resultados para o OpenMetadata."""
        for result in self.results:
            self.client.register_data_quality(result)
        
        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed
        print(f"[GOVERNANCE] DQ results: {passed} passed, {failed} failed")


# ==============================================================================
# Funcoes de conveniencia para uso nos notebooks
# ==============================================================================

def register_pipeline_lineage(source_layer: str, target_layer: str, 
                               transformations: List[Dict[str, str]]):
    """
    Registra lineage de um pipeline completo.
    
    Args:
        source_layer: Camada de origem
        target_layer: Camada de destino
        transformations: Lista de transformacoes
            [{"source": "bronze_sales", "target": "silver_sales", "desc": "..."}]
    """
    tracker = LineageTracker(source_layer, target_layer)
    
    for t in transformations:
        tracker.add_table_transformation(
            t["source"], 
            t["target"], 
            t.get("desc", "Transformation")
        )
    
    tracker.commit()


def register_table_metadata(layer: str, table_name: str, description: str,
                            columns: List[Dict[str, str]], tags: List[str] = None):
    """
    Registra metadata de uma tabela.
    
    Args:
        layer: Camada (bronze, silver, gold, consumption)
        table_name: Nome da tabela
        description: Descricao
        columns: Lista de colunas [{"name": "col1", "type": "STRING", "description": "..."}]
        tags: Tags opcionais
    """
    client = OpenMetadataClient()
    
    metadata = TableMetadata(
        name=table_name,
        layer=layer,
        database="abinbev",
        description=description,
        columns=columns,
        tags=tags or []
    )
    
    client.register_table(metadata)


def report_dq_results(table_name: str, checks: List[Dict[str, Any]]):
    """
    Reporta resultados de DQ.
    
    Args:
        table_name: Nome da tabela
        checks: Lista de checks
            [{"name": "null_check", "type": "completeness", "passed": True, "value": 0.99, "threshold": 0.95}]
    """
    reporter = DataQualityReporter(table_name)
    
    for check in checks:
        reporter.report_check(
            check["name"],
            check["type"],
            check["passed"],
            check["value"],
            check["threshold"]
        )
    
    reporter.commit()

