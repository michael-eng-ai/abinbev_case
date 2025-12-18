# Demo - Apresentação Local
# ========================
# Esta pasta contém arquivos para demonstração local do ambiente
# e NÃO deve ser versionada no GitHub.

## Conteúdo
- `00_demo_presentation.ipynb` - Notebook interativo de apresentação
- `quick_analysis.py` - Script de análise rápida via terminal
- `README.md` - Este arquivo

## Como Usar

### Notebook Interativo
```bash
# Ative o ambiente virtual
source venv/bin/activate

# Inicie o Jupyter
jupyter notebook demo/00_demo_presentation.ipynb
```

### Análise Rápida via Terminal
```bash
# Ative o ambiente virtual
source venv/bin/activate

# Execute o script
python demo/quick_analysis.py
```

## Arquitetura Demonstrada

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PIPELINE MEDALLION - ABInBev                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  📥 LANDING     →  🥉 BRONZE    →  🥈 SILVER    →  🥇 GOLD          │
│  (CSV Raw)         (Ingested)      (Cleaned)       (Business)       │
│                                                                     │
│                                           ↓                         │
│                                                                     │
│                              💎 CONSUMPTION (Star Schema)            │
│                              ├── dim_time                           │
│                              ├── dim_product                        │
│                              ├── dim_region                         │
│                              ├── dim_channel                        │
│                              ├── fact_sales                         │
│                              └── aggregations                       │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Business Queries Respondidas

1. **Top 3 Trade Groups** - Grupos de comércio com maior volume
2. **Vendas por Mês** - Evolução temporal das vendas
3. **Menor Marca por Região** - Identificação de underperformers

## Notas Importantes

⚠️ **Esta pasta é ignorada pelo Git** - Os arquivos aqui são apenas para demonstração local.

📊 **Dados necessários** - Certifique-se de que o pipeline foi executado e os dados existem em `data/`.

🔧 **Dependências** - Requer PySpark, Pandas, Matplotlib, Seaborn instalados.
