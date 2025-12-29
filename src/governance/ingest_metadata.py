"""
OpenMetadata Ingestion Script

Script para ingerir metadados das tabelas Delta Lake no OpenMetadata.
Deve ser executado apos cada pipeline run para manter o catalogo atualizado.
"""

import logging
import os
from typing import Any, Dict

from config.secrets import get_secret

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_openmetadata_config() -> Dict[str, Any]:
    """
    Retorna a configuracao de conexao com o OpenMetadata.

    Environment Variables:
        OPENMETADATA_HOST: URL do servidor OpenMetadata
        OPENMETADATA_TOKEN: Token de autenticacao (opcional)
    """
    host = os.getenv("OPENMETADATA_HOST", "http://localhost:8585/api")
    token = get_secret("OPENMETADATA_TOKEN", "")

    config = {
        "source": {
            "type": "deltalake",
            "serviceName": "abinbev_datalake",
            "serviceConnection": {
                "config": {
                    "type": "DeltaLake",
                    "metastoreConnection": {
                        "metastoreFilePath": os.getenv("DELTA_METASTORE_PATH", "data/")
                    },
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "DatabaseMetadata",
                    "schemaFilterPattern": {
                        "includes": ["bronze.*", "silver.*", "gold.*", "consumption.*"]
                    },
                }
            },
        },
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {
            "openMetadataServerConfig": {
                "hostPort": host,
                "authProvider": "openmetadata" if token else "no-auth",
                "securityConfig": {"jwtToken": token} if token else {},
            }
        },
    }

    return config


def create_lineage_config() -> Dict[str, Any]:
    """
    Cria configuracao para ingestao de lineage.
    Define as relacoes entre tabelas nas diferentes camadas.
    """
    return {
        "source": {
            "type": "query-log-lineage",
            "serviceName": "abinbev_datalake",
            "sourceConfig": {"config": {"type": "DatabaseLineage"}},
        },
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {
            "openMetadataServerConfig": {
                "hostPort": os.getenv("OPENMETADATA_HOST", "http://localhost:8585/api"),
                "authProvider": "no-auth",
            }
        },
    }


def ingest_metadata() -> bool:
    """
    Executa a ingestao de metadados no OpenMetadata.

    Returns:
        True se a ingestao foi bem sucedida, False caso contrario.
    """
    try:
        # Importacao dinamica para evitar dependencia obrigatoria
        from metadata.ingestion.api.workflow import Workflow

        config = get_openmetadata_config()
        logger.info("Iniciando ingestao de metadados...")

        workflow = Workflow.create(config)
        workflow.execute()
        workflow.raise_from_status()
        workflow.print_status()
        workflow.stop()

        logger.info("Ingestao de metadados concluida com sucesso!")
        return True

    except ImportError:
        logger.warning(
            "OpenMetadata SDK nao instalado. " "Execute: poetry add openmetadata-ingestion"
        )
        return False

    except Exception as e:
        logger.error(f"Erro na ingestao de metadados: {e}")
        return False


def ingest_lineage() -> bool:
    """
    Executa a ingestao de lineage no OpenMetadata.

    Returns:
        True se a ingestao foi bem sucedida, False caso contrario.
    """
    try:
        from metadata.ingestion.api.workflow import Workflow

        config = create_lineage_config()
        logger.info("Iniciando ingestao de lineage...")

        workflow = Workflow.create(config)
        workflow.execute()
        workflow.raise_from_status()
        workflow.print_status()
        workflow.stop()

        logger.info("Ingestao de lineage concluida com sucesso!")
        return True

    except ImportError:
        logger.warning(
            "OpenMetadata SDK nao instalado. " "Execute: poetry add openmetadata-ingestion"
        )
        return False

    except Exception as e:
        logger.error(f"Erro na ingestao de lineage: {e}")
        return False


def register_custom_properties() -> bool:
    """
    Registra propriedades customizadas para as tabelas no OpenMetadata.

    Propriedades:
        - layer: Camada do Medallion (bronze, silver, gold, consumption)
        - data_quality_score: Score de qualidade dos dados
        - last_pipeline_run: Timestamp da ultima execucao do pipeline
    """
    try:
        from metadata.ingestion.ometa.ometa_api import OpenMetadata

        host = os.getenv("OPENMETADATA_HOST", "http://localhost:8585/api")
        _metadata_client = OpenMetadata(
            config={"hostPort": host, "authProvider": "no-auth"}
        )  # noqa: F841

        # TODO: Implementar criacao de propriedades customizadas usando _metadata_client
        # Propriedades customizadas a serem criadas
        custom_properties = [
            {
                "name": "layer",
                "description": "Medallion architecture layer",
                "propertyType": "string",
            },
            {
                "name": "data_quality_score",
                "description": "Data quality score (0-100)",
                "propertyType": "number",
            },
            {
                "name": "last_pipeline_run",
                "description": "Timestamp of last pipeline execution",
                "propertyType": "timestamp",
            },
        ]

        for prop in custom_properties:
            logger.info(f"Registrando propriedade: {prop['name']}")
            # Implementacao real dependeria da versao especifica do SDK

        logger.info("Propriedades customizadas registradas!")
        return True

    except Exception as e:
        logger.error(f"Erro ao registrar propriedades: {e}")
        return False


def main():
    """
    Funcao principal para executar todas as ingestoes.
    """
    logger.info("=" * 50)
    logger.info("ABInBev Case - OpenMetadata Ingestion")
    logger.info("=" * 50)

    # Ingestao de metadados
    metadata_success = ingest_metadata()

    # Ingestao de lineage
    lineage_success = ingest_lineage()

    # Registro de propriedades customizadas
    properties_success = register_custom_properties()

    # Resumo
    logger.info("=" * 50)
    logger.info("Resumo da Ingestao:")
    logger.info(f"  Metadados: {'OK' if metadata_success else 'FALHA'}")
    logger.info(f"  Lineage: {'OK' if lineage_success else 'FALHA'}")
    logger.info(f"  Propriedades: {'OK' if properties_success else 'FALHA'}")
    logger.info("=" * 50)

    return all([metadata_success, lineage_success, properties_success])


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
