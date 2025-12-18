#!/bin/bash
# ============================================================
# Setup do Ambiente de Demo - ABInBev Case
# ============================================================
# Este script configura o ambiente para executar a demonstração
# local do pipeline de dados.
#
# Uso: source demo/setup_demo.sh
# ============================================================

echo "[INFO] Configurando ambiente de demo..."

# Configura Java 17 (necessário para Spark)
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

echo "[OK] JAVA_HOME configurado: $JAVA_HOME"
echo "[OK] Java Version: $(java -version 2>&1 | head -1)"

# Define o diretório do projeto
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo "[OK] Diretório do projeto: $PROJECT_ROOT"

# Ativa o ambiente virtual
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "[OK] Ambiente virtual ativado"
else
    echo "[WARN] Ambiente virtual não encontrado. Execute: python -m venv venv"
fi

# Verifica dependências
echo ""
echo "[INFO] Verificando dependências..."
python -c "import pyspark; print(f'  [OK] PySpark: {pyspark.__version__}')" 2>/dev/null || echo "  [ERROR] PySpark não instalado"

echo ""
echo "============================================================"
echo "[SUCCESS] Ambiente configurado!"
echo ""
echo "Comandos disponíveis:"
echo "  • python demo/quick_analysis.py  - Análise rápida"
echo "  • jupyter notebook demo/00_demo_presentation.ipynb  - Notebook"
echo "============================================================"
