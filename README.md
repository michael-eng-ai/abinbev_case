# ABInBev Data Engineering Case
## Beverage Sales Analytics Platform

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.4+-orange.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.4+-green.svg)](https://delta.io/)
[![Terraform](https://img.shields.io/badge/Terraform-1.0+-purple.svg)](https://www.terraform.io/)
[![Azure](https://img.shields.io/badge/Azure-HDInsight-blue.svg)](https://azure.microsoft.com/)

---

## Sumario

- [Sobre o Projeto](#sobre-o-projeto)
- [Arquitetura](#arquitetura)
- [Tecnologias](#tecnologias)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Como Executar](#como-executar)
- [Modelo de Dados](#modelo-de-dados)
- [Queries de Negocio](#queries-de-negocio)
- [Proximos Passos](#proximos-passos)

---

## Sobre o Projeto

Plataforma de analytics para dados de vendas de bebidas, utilizando:
- Arquitetura **Medallion** (Landing -> Bronze -> Silver -> Gold -> Consumption)
- Processamento incremental com **CDC** (PK + row_hash)
- **Azure HDInsight** para processamento distribuido
- **Delta Lake** para ACID e Time Travel
- **OpenMetadata** para governanca de dados
- **Terraform** para infraestrutura como codigo

### Objetivos

1. **Pipeline de Ingestao**: Merge dos dados de vendas com features de canais
2. **Modelagem Dimensional**: Dimensoes, fatos e tabelas de agregacao
3. **Queries de Negocio**: Responder as perguntas analiticas do case
4. **Processamento Incremental**: CDC para detectar e processar apenas mudancas

---

## Arquitetura

```
+-----------------------------------------------------------------------------------+
|                              AZURE CLOUD                                           |
+-----------------------------------------------------------------------------------+
|                                                                                    |
|  +-----------------------------------------------------------------------------+  |
|  |                     GOVERNANCE (OpenMetadata)                                |  |
|  |  Catalog | Lineage | Data Quality | Policies | Glossary                      |  |
|  +-----------------------------------------------------------------------------+  |
|                                      |                                            |
|  +-----------------------------------------------------------------------------+  |
|  |                     OBSERVABILITY (Prometheus + Grafana + Loki)              |  |
|  |  Metrics | Dashboards | Alerts | Logs                                        |  |
|  +-----------------------------------------------------------------------------+  |
|                                      |                                            |
|  +-----------------------------------------------------------------------------+  |
|  |                     DATA LAKE (ADLS Gen2 + Delta Lake)                       |  |
|  |                                                                              |  |
|  |   LANDING     BRONZE        SILVER         GOLD         CONSUMPTION          |  |
|  |   (CSV)  -->  (Delta)  -->  (Delta)  -->  (Delta)  -->  (Delta)              |  |
|  |               + PK          + UPSERT      + Rules       + Star Schema        |  |
|  |               + hash        + DQ          + Enrich      + Aggregations       |  |
|  |               (append)      (current)                                        |  |
|  |                                                                              |  |
|  +-----------------------------------------------------------------------------+  |
|                                      |                                            |
|  +------------------+    +------------------+    +------------------+             |
|  |   HDInsight      |    | Apache Airflow   |    |   Terraform      |             |
|  |   (Spark 3.5)    |    | (Orchestration)  |    |   (IaC)          |             |
|  |   Autoscaling    |    | Sequential       |    |                  |             |
|  +------------------+    +------------------+    +------------------+             |
|                                                                                    |
+-----------------------------------------------------------------------------------+
```

### Camadas

| Camada | Formato | Proposito | Modo |
|--------|---------|-----------|------|
| **Landing** | CSV | Dados brutos | - |
| **Bronze** | Delta | Historico completo + PK + row_hash | APPEND |
| **Silver** | Delta | Dados limpos, UPSERT incremental | MERGE |
| **Gold** | Delta | Regras de negocio, enriquecimento | OVERWRITE |
| **Consumption** | Delta | Modelo dimensional (Star Schema) | OVERWRITE |

### CDC (Change Data Capture)

```
Bronze (historico)              Silver (atual)
+------------------------+      +------------------------+
| _pk | _row_hash | data |      | _pk | _row_hash | data |
| abc | xyz       | v1   |  --> | abc | uvw       | v2   |
| abc | uvw       | v2   |      +------------------------+
+------------------------+      (mantém só o mais recente)
```

---

## Tecnologias

### Stack Principal

| Componente | Tecnologia | Motivo |
|------------|------------|--------|
| Cloud | Azure | Requisito do case |
| Processamento | HDInsight Spark 3.5 | Portabilidade entre clouds |
| Orquestracao | Apache Airflow | Pipelines-as-code, padrao de mercado |
| Storage | ADLS Gen2 + Delta Lake | ACID, Time Travel, Schema Evolution |
| Governanca | OpenMetadata | Open source, catalogo + lineage |
| Observabilidade | Prometheus + Grafana + Loki + OpenTelemetry | Open source, portavel |
| IaC | Terraform | Reproducibilidade |
| CI/CD | GitHub Actions | Automacao de testes e deploy |
| Dependencias | Poetry | Builds reprodutiveis |

### Por que essas escolhas?

| Decisao | Alternativa | Motivo da Escolha |
|---------|-------------|-------------------|
| HDInsight | Databricks | Portabilidade, sem vendor lock-in |
| Apache Airflow | Azure Data Factory | Pipelines-as-code, multi-cloud |
| OpenMetadata | Unity Catalog | Open source, multi-cloud |
| Prometheus/Grafana | Azure Monitor | Open source, portavel |
| Poetry | pip/requirements.txt | Builds reprodutiveis, lockfile |
| Batch | Streaming | Requisitos do case |

---

## Estrutura do Projeto

```
abinbev_case/
|
+-- .github/
|   +-- workflows/
|       +-- ci.yml                    # CI/CD Pipeline
|
+-- dags/
|   +-- abinbev_case_pipeline.py      # Airflow DAG
|
+-- infrastructure/
|   +-- terraform/
|       +-- main.tf
|       +-- variables.tf
|       +-- outputs.tf
|       +-- terraform.tfvars.example
|       +-- modules/
|           +-- storage/
|           +-- hdinsight/
|           +-- observability/
|           +-- governance/
|
+-- notebooks/
|   +-- 01_bronze_ingestion.py
|   +-- 02_silver_transformation.py
|   +-- 03_gold_business_rules.py
|   +-- 04_consumption_dimensional.py
|
+-- src/
|   +-- transformations/              # Funcoes reutilizaveis
|   |   +-- bronze_layer.py
|   |   +-- silver_layer.py
|   |   +-- gold_layer.py
|   +-- governance/
|       +-- ingest_metadata.py        # OpenMetadata integration
|
+-- config/
|   +-- config.yaml
|   +-- prometheus.yml
|   +-- alert_rules.yml
|   +-- grafana_spark_dashboard.json
|
+-- docs/
|   +-- ARCHITECTURE.md
|   +-- DATA_DICTIONARY.md
|   +-- DEVELOPMENT_JOURNAL.md
|
+-- tests/
|   +-- conftest.py
|   +-- test_transformations.py
|
+-- pyproject.toml                    # Poetry dependencies
+-- .gitignore
+-- README.md
+-- LICENSE
```

---

## Como Executar

### Pre-requisitos

- Python 3.10+
- Java 11 (para Spark)
- Poetry (gerenciador de dependencias)
- Terraform 1.0+ (para deploy Azure)

### Execucao Local com Poetry

```bash
# 1. Clonar repositorio
git clone https://github.com/seu-usuario/abinbev_case.git
cd abinbev_case

# 2. Instalar dependencias
pip install poetry
poetry install

# 3. Configurar variaveis (opcional)
cp config/env.example .env
# Editar .env se necessario

# 4. Executar pipeline completo
poetry run python notebooks/01_bronze_ingestion.py
poetry run python notebooks/02_silver_transformation.py
poetry run python notebooks/03_gold_business_rules.py
poetry run python notebooks/04_consumption_dimensional.py
```

### Executar Testes

```bash
# Rodar todos os testes
poetry run pytest tests/ -v

# Com coverage
poetry run pytest tests/ --cov=src --cov-report=term-missing
```

### Execucao Individual

```bash
# Bronze (Landing -> Bronze)
python notebooks/01_bronze_ingestion.py

# Silver (Bronze -> Silver + DQ)
python notebooks/02_silver_transformation.py

# Gold (Silver -> Gold + Business Rules)
python notebooks/03_gold_business_rules.py

# Consumption (Gold -> Dimensional Model)
python notebooks/04_consumption_dimensional.py
```

### Deploy Azure (Terraform)

```bash
cd infrastructure/terraform

# Configurar variaveis
cp terraform.tfvars.example terraform.tfvars
# Editar terraform.tfvars

# Deploy
terraform init
terraform plan
terraform apply
```

---

## Modelo de Dados

### Star Schema (Consumption Layer)

```
                    +-------------+
                    |  dim_date   |
                    +------+------+
                           |
+-------------+     +------+------+     +-------------+
| dim_region  |---->| fact_sales  |<----| dim_product |
+-------------+     +------+------+     +-------------+
                           |
                    +------+------+
                    | dim_channel |
                    +-------------+
```

### Dimensoes

| Dimensao | Descricao | Campos Principais |
|----------|-----------|-------------------|
| dim_date | Calendario | date_key, year, month, quarter |
| dim_region | Regioes | region_key, region_name, country |
| dim_product | Produtos | product_key, brand_nm, pkg_cat |
| dim_channel | Canais | channel_key, trade_group, trade_type |

### Fato

| Fato | Metricas | Grain |
|------|----------|-------|
| fact_sales | dollar_volume | Transacao por produto/regiao/canal/mes |

### Agregacoes

| Tabela | Proposito |
|--------|-----------|
| agg_sales_region_tradegroup | Top Trade Groups por Regiao |
| agg_sales_brand_month | Vendas por Marca por Mes |
| agg_sales_brand_region | Vendas por Marca por Regiao |

---

## Queries de Negocio

### 4.1 Top 3 Trade Groups por Regiao ($ Volume)

```sql
SELECT 
    region_name,
    trade_group_desc,
    total_dollar_volume,
    rank
FROM consumption.agg_sales_region_tradegroup
WHERE rank <= 3
ORDER BY region_name, rank;
```

### 4.2 Vendas por Marca por Mes

```sql
SELECT 
    brand_nm,
    year_month,
    total_dollar_volume,
    transaction_count
FROM consumption.agg_sales_brand_month
ORDER BY brand_nm, year_month;
```

### 4.3 Marca com Menor Venda por Regiao

```sql
SELECT 
    region_name,
    brand_nm,
    total_dollar_volume
FROM consumption.agg_sales_brand_region
WHERE rank_asc = 1
ORDER BY region_name;
```

---

## Campos de Auditoria

Todos os registros possuem rastreabilidade completa:

| Campo | Proposito |
|-------|-----------|
| _source_file | Arquivo de origem |
| _ingestion_date | Data da ingestao |
| _updated_at | Ultima atualizacao (atualizado em cada camada) |
| _layer | Camada atual |
| _pk | Primary Key (hash MD5) |
| _row_hash | Hash das colunas (SHA256) para CDC |

---

## Proximos Passos

### Concluido
- [x] CI/CD com GitHub Actions
- [x] Apache Airflow para orquestracao
- [x] Modulo de transformacoes reutilizavel
- [x] Testes unitarios com pytest
- [x] Poetry para gerenciamento de dependencias
- [x] Configuracao de observabilidade (Prometheus/Grafana)
- [x] Integracao com OpenMetadata

### Curto Prazo
- [ ] Great Expectations para data quality
- [ ] Dashboard Power BI
- [ ] Testes end-to-end

### Medio Prazo
- [ ] SCD Type 2 nas dimensoes
- [ ] Delta Live Tables

### Longo Prazo
- [ ] Streaming com Kafka
- [ ] ML para previsao de demanda
- [ ] Data Mesh

---

## Documentacao Adicional

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) - Arquitetura detalhada
- [DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md) - Dicionario de dados
- [DEVELOPMENT_JOURNAL.md](docs/DEVELOPMENT_JOURNAL.md) - Historico de desenvolvimento

---

## Autor

**Michael Santos**  
Data Engineer

---

## Licenca

Este projeto esta sob a licenca MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.
