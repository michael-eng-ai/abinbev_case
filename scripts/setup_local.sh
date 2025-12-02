#!/bin/bash
# ==============================================================================
# ABInBev Case - Setup Local Environment
# ==============================================================================
#
# Este script configura o ambiente local para desenvolvimento e testes.
#
# Uso:
#   chmod +x scripts/setup_local.sh
#   ./scripts/setup_local.sh
#
# ==============================================================================

set -e

echo "=============================================="
echo "ABInBev Case - Setup Local Environment"
echo "=============================================="

# Diretorio raiz do projeto
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo ""
echo "[1/5] Criando virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "      Virtual environment criado."
else
    echo "      Virtual environment ja existe."
fi

echo ""
echo "[2/5] Ativando virtual environment..."
source venv/bin/activate

echo ""
echo "[3/5] Instalando dependencias..."
pip install --upgrade pip
pip install -r requirements.txt

echo ""
echo "[4/5] Criando estrutura de diretorios..."
mkdir -p data/{landing,bronze,silver,gold,consumption,control}
mkdir -p logs

echo ""
echo "[5/5] Configurando arquivo .env..."
if [ ! -f ".env" ]; then
    if [ -f "config/env.example" ]; then
        cp config/env.example .env
        echo "      Arquivo .env criado a partir do config/env.example"
    else
        echo "      Criando .env padrao..."
        cat > .env << EOF
# ABInBev Case - Environment Variables
ENVIRONMENT=local
SPARK_DRIVER_MEMORY=4g
AZURE_STORAGE_ACCOUNT_NAME=abinbevdatalake
GOVERNANCE_ENABLED=false
EOF
    fi
    echo "      IMPORTANTE: Edite o arquivo .env com suas configuracoes"
else
    echo "      Arquivo .env ja existe."
fi

echo ""
echo "=============================================="
echo "Setup concluido!"
echo "=============================================="
echo ""
echo "Proximos passos:"
echo "  1. Ative o virtual environment:"
echo "     source venv/bin/activate"
echo ""
echo "  2. Edite o arquivo .env com suas configuracoes"
echo ""
echo "  3. Execute os notebooks:"
echo "     jupyter notebook notebooks/"
echo ""
echo "  Ou execute os scripts diretamente:"
echo "     python notebooks/01_bronze_ingestion.py"
echo ""

