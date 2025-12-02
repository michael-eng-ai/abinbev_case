#!/bin/bash
# ==============================================================================
# ABInBev Case - Run Complete Pipeline
# ==============================================================================
#
# Este script executa o pipeline completo localmente:
# 1. Bronze Ingestion
# 2. Silver Transformation
# 3. Gold Business Rules
# 4. Consumption Dimensional
#
# Uso:
#   chmod +x scripts/run_pipeline.sh
#   ./scripts/run_pipeline.sh
#
# ==============================================================================

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=============================================="
echo "ABInBev Case - Pipeline Completo"
echo "=============================================="
echo "Projeto: $PROJECT_ROOT"
echo "Data: $(date)"
echo ""

# Ativa venv se existir
if [ -d "venv" ]; then
    echo "[INFO] Ativando virtual environment..."
    source venv/bin/activate
fi

# Ajusta opcoes de JVM para compatibilidade com JDK 21+
JAVA_OPEN_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.security=ALL-UNNAMED \
--add-opens=java.base/javax.security.auth=ALL-UNNAMED \
-Djava.security.manager=allow"

export SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS} ${JAVA_OPEN_OPTS}"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} ${JAVA_OPEN_OPTS}"

# Verifica se .env existe
if [ ! -f ".env" ]; then
    echo "[WARN] Arquivo .env nao encontrado. Usando config/env.example..."
    cp config/env.example .env
fi

# Cria diretorios se nao existirem
echo "[INFO] Criando estrutura de diretorios..."
mkdir -p data/{landing,bronze,silver,gold,consumption,control}

echo ""
echo "=============================================="
echo "ETAPA 1/4: Bronze Ingestion"
echo "=============================================="
python notebooks/01_bronze_ingestion.py

echo ""
echo "=============================================="
echo "ETAPA 2/4: Silver Transformation"
echo "=============================================="
python notebooks/02_silver_transformation.py

echo ""
echo "=============================================="
echo "ETAPA 3/4: Gold Business Rules"
echo "=============================================="
python notebooks/03_gold_business_rules.py

echo ""
echo "=============================================="
echo "ETAPA 4/4: Consumption Dimensional"
echo "=============================================="
python notebooks/04_consumption_dimensional.py

echo ""
echo "=============================================="
echo "PIPELINE CONCLUIDO COM SUCESSO!"
echo "=============================================="
echo ""
echo "Dados disponiveis em:"
echo "  - data/bronze/"
echo "  - data/silver/"
echo "  - data/gold/"
echo "  - data/consumption/"
echo ""

