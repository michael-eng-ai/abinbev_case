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

## Como Executar (Quick Start)

Este projeto foi desenhado para rodar localmente simulando um ambiente Enterprise completo.

### Pré-requisitos
- Python 3.10+
- Java 11+ (necessário para Spark)
- Poetry (Gerenciador de Dependências)

### 1. Instalação
```bash
# Clone o repositório
git clone https://github.com/seu-usuario/abinbev_case.git
cd abinbev_case

# Instale as dependências (incluindo Airflow e Jupyter)
poetry install --with dev
```

### 2. Executando a Demonstração
Para uma experiência completa, recomendamos seguir o **[Guia de Demonstração](demo/demo_guide.md)**, que orquestra o Airflow e o Jupyter Notebook.

**Resumo Rápido (Apenas Pipeline):**
```bash
# Executa todas as camadas sequencialmente via script
poetry run python notebooks/01_bronze_ingestion.py
poetry run python notebooks/02_silver_transformation.py
poetry run python notebooks/03_gold_business_rules.py
poetry run python notebooks/04_consumption_dimensional.py
```

### 3. Orquestração com Airflow (Local)
Para replicar a orquestração oficial via Airflow:

**1. Configuração Inicial (Primeira execução)**
```bash
export AIRFLOW_HOME="$(pwd)/airflow_local"

# Inicializa o banco de dados e cria usuário admin
poetry run airflow db migrate
poetry run airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

**2. Iniciando os Serviços**
É necessário rodar em terminais separados:

*Terminal 1 (Webserver):*
```bash
export AIRFLOW_HOME="$(pwd)/airflow_local"
poetry run airflow webserver -p 8080
```

*Terminal 2 (Scheduler):*
```bash
export AIRFLOW_HOME="$(pwd)/airflow_local"
# Defina JAVA_HOME se necessário
poetry run airflow scheduler
```
Acesse: [http://localhost:8080](http://localhost:8080) (Login: `admin` / `admin`)

### 4. Explorando os Dados
```bash
# Abra o notebook de consultas interativas
poetry run jupyter notebook notebooks/interactive_queries.ipynb
```

---

## Estrutura de Pastas
- `dags/`: Definições do pipeline Airflow.
- `notebooks/`: Scripts PySpark de transformação e análise.
- `src/`: Código modularizado (Transformações, Governança, Controle).
- `config/`: Arquivos de configuração (YAML) para políticas e alertas.
- `demo/`: Guias e materiais de apresentação.
- `data/`: (Criado automaticamente) Variável local simulando Data Lake.

---


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
