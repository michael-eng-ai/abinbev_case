# Caderno de Desenvolvimento - ABInBev Data Engineering Case

## Informacoes do Projeto

| Item | Valor |
|------|-------|
| Candidato | Michael Santos |
| Data Inicio | 30/11/2024 |
| Prazo | Ate 4 dias uteis |
| Apresentacao | 45 min (30 min solucao + 15 min Q&A) |

---

## Resumo Executivo do Case

**Objetivo**: Construir um MVP para uma plataforma de Analytics de Vendas de Bebidas

**Requisitos**:
1. Data Pipeline para merge de Sales + Channel Features
2. Modelo Dimensional (Dimensoes + Fatos + Tabelas Summary)
3. Proximos passos e melhorias futuras
4. Queries para responder 3 perguntas de negocio

**Stack Implementada**:
- Cloud: Azure (HDInsight + ADLS Gen2)
- Orquestracao: Apache Airflow (pipelines-as-code)
- Processamento: PySpark
- Formato: Delta Lake
- Observabilidade: Prometheus + Grafana + Loki
- Governanca: OpenMetadata
- IaC: Terraform
- CDC: PK + row_hash para processamento incremental

---

## Sessao 1: Analise Exploratoria dos Dados

### Data: 30/11/2024

### 1.1 Descobertas Criticas nos Arquivos Fonte

**Arquivo: abi_bus_case1_beverage_sales_20210726.csv**
- Encoding: UTF-16 Little-Endian (CRLF)
- Registros: 16.151 linhas
- Delimitador: TAB
- Problema encontrado: BRAND_NM tem espaco no inicio

**Arquivo: abi_bus_case1_beverage_channel_group_20210726.csv**
- Encoding: UTF-8 CSV padrao
- Registros: 31 linhas
- Delimitador: COMMA

### 1.2 Metricas dos Dados

| Metrica | Valor |
|---------|-------|
| Total registros vendas | 16.151 |
| Total registros canais | 31 |
| Periodo dos dados | 2006 (Ano unico) |
| Regioes unicas | 7 (CANADA, GREAT LAKES, MIDWEST, NORTHEAST, SOUTHEAST, SOUTHWEST, WEST) |
| Marcas unicas | 4 (GRAPE, LEMON, RASPBERRY, STRAWBERRY) |
| Trade Groups unicos | 6 (ENTERTAINMENT, SERVICES, GROCERY, OTHER, GOV & MILITARY, ACADEMIC) |

---

## Sessao 2: Arquitetura Implementada

### 2.1 Camadas

| Camada | Proposito |
|--------|-----------|
| Landing | Arquivos brutos (CSV) |
| Bronze | Schema-on-read + PK + row_hash + auditoria (historico completo) |
| Silver | Data Quality + UPSERT incremental (dado mais atual) |
| Gold | Regras de negocio + campos calculados |
| Consumption | Modelo dimensional (Star Schema) |

### 2.3 CDC (Change Data Capture)

| Conceito | Implementacao |
|----------|---------------|
| PK (Primary Key) | Hash MD5 das colunas de identificacao |
| row_hash | Hash SHA256 de todas as colunas de negocio |
| _updated_at | Timestamp para resolver conflitos |
| Bronze | Append (historico completo) |
| Silver | Upsert (dado mais atual) |

**Logica de Merge na Silver**:
1. Se PK nao existe: INSERT
2. Se PK existe e hash diferente e mais recente: UPDATE
3. Se PK existe e hash igual: SKIP

### 2.2 Decisoes de Arquitetura

| Decisao | Motivo |
|---------|--------|
| Azure HDInsight (nao Databricks) | Portabilidade entre clouds |
| Delta Lake | ACID, Time Travel, Schema Evolution |
| Prometheus + Grafana + Loki | Open source, portabilidade |
| Terraform | Infraestrutura como codigo |
| Batch (nao real-time) | Requisitos do case |

---

## Sessao 3: Implementacao

### 3.1 Terraform

| Modulo | Recursos |
|--------|----------|
| storage | ADLS Gen2 com containers |
| hdinsight | Cluster Spark com autoscaling |
| data_factory | Pipelines e triggers |
| observability | Prometheus, Grafana, Loki |
| governance | OpenMetadata, PostgreSQL, Elasticsearch |

### 3.2 Notebooks

| Notebook | Descricao |
|----------|-----------|
| 01_bronze_ingestion.py | Landing -> Bronze |
| 02_silver_transformation.py | Bronze -> Silver + DQ |
| 03_gold_business_rules.py | Silver -> Gold |
| 04_consumption_dimensional.py | Gold -> Consumption |

### 3.3 Regras de Negocio (Gold Layer)

| Campo | Logica |
|-------|--------|
| volume_category | HIGH (>1000), MEDIUM (>100), LOW (<=100) |
| is_negative_sale | True se dollar_volume < 0 |
| region_group | US_EAST, US_WEST, CANADA |
| sales_quarter | Q1, Q2, Q3, Q4 |

### 3.4 Modelo Dimensional (Consumption Layer)

| Tipo | Tabela |
|------|--------|
| Dimensao | dim_date, dim_region, dim_product, dim_channel |
| Fato | fact_sales |
| Agregacao | agg_sales_region_tradegroup, agg_sales_brand_month, agg_sales_brand_region |

---

## Sessao 4: Perguntas Esperadas na Apresentacao

### Perguntas Tecnicas

| Pergunta | Resposta |
|----------|----------|
| Por que Arquitetura Medallion? | Separacao de responsabilidades, reprocessamento, debugging |
| Por que HDInsight e nao Databricks? | Portabilidade entre clouds, sem lock-in |
| Por que Delta Lake? | ACID, Time Travel, Schema Evolution, open source |
| Como tratou Data Quality? | Regras especificas por arquivo, quarentena, alertas |
| Por que Batch e nao Real-Time? | Requisitos do case nao exigem streaming |
| Por que Prometheus + Grafana? | Open source, portabilidade, sem vendor lock-in |
| Por que OpenMetadata? | Open source, moderno, catalogo + lineage + DQ integrados |
| Por que PK + row_hash? | Processamento incremental eficiente, detecta mudancas |
| Por que Bronze append e Silver upsert? | Bronze = historico completo, Silver = dado atual |

### Perguntas de Negocio

| Pergunta | Resposta |
|----------|----------|
| O que sao Trade Groups? | Agrupamentos de canais por tipo (GROCERY, ENTERTAINMENT, etc) |
| Por que valores negativos? | Devolucoes/estornos - mantidos para refletir realidade |
| Por que region_group? | Simplificar analises por macro-regiao |

---

## Sessao 5: Log de Desenvolvimento

| Data | Atividade | Status |
|------|-----------|--------|
| 30/11/2024 | Analise exploratoria dos dados | Concluido |
| 30/11/2024 | Descoberta encoding UTF-16 | Concluido |
| 01/12/2024 | Arquitetura Landing + Bronze | Concluido |
| 01/12/2024 | Arquitetura Silver + Data Quality | Concluido |
| 01/12/2024 | Arquitetura Gold + Regras de Negocio | Concluido |
| 01/12/2024 | Arquitetura Consumption + Dimensional | Concluido |
| 01/12/2024 | DATA_DICTIONARY atualizado | Concluido |
| 01/12/2024 | Terraform implementado | Concluido |
| 01/12/2024 | Notebooks implementados (local + cloud) | Concluido |
| 01/12/2024 | Governanca (OpenMetadata) | Concluido |
| 01/12/2024 | CDC (PK + row_hash) implementado | Concluido |
| 01/12/2024 | UPSERT incremental na Silver | Concluido |
| 02/12/2024 | Migracao ADF -> Apache Airflow | Concluido |
| 02/12/2024 | CI/CD com GitHub Actions | Concluido |
| 02/12/2024 | Poetry para gerenciamento de dependencias | Concluido |
| 02/12/2024 | Modulos de transformacao reutilizaveis | Concluido |
| 02/12/2024 | ProcessControl integrado aos notebooks | Concluido |
| 02/12/2024 | Testes unitarios com pytest | Concluido |
| 02/12/2024 | Revisao geral codigo vs documentacao | Concluido |

---

## Sessao 6: Estrutura do Projeto

```
abinbev_case/
  .github/
    workflows/
      ci.yml                    # CI/CD Pipeline
  dags/
    abinbev_case_pipeline.py    # Airflow DAG
  infrastructure/
    terraform/
      main.tf
      variables.tf
      outputs.tf
      terraform.tfvars.example
      modules/
        storage/
        hdinsight/
        airflow/                # Substituiu data_factory
        observability/
        governance/
  notebooks/
    01_bronze_ingestion.py      # Com ProcessControl
    02_silver_transformation.py # Com ProcessControl
    03_gold_business_rules.py   # Com ProcessControl
    04_consumption_dimensional.py # Com ProcessControl
  src/
    transformations/            # Funcoes reutilizaveis
      bronze_layer.py
      silver_layer.py
      gold_layer.py
    control/
      process_control.py        # Tabela de controle
    governance/
      ingest_metadata.py
  config/
    config.yaml
    prometheus.yml
    alert_rules.yml
    grafana_spark_dashboard.json
  tests/
    conftest.py
    test_transformations.py
  docs/
    ARCHITECTURE.md
    DATA_DICTIONARY.md
    DEVELOPMENT_JOURNAL.md
  pyproject.toml               # Poetry
  .gitignore
```

---

## Sessao 7: Proximos Passos

### Melhorias Tecnicas
1. Delta Live Tables para orquestracao
2. SCD Type 2 para dimensoes
3. CI/CD com Azure DevOps
4. Streaming com Kafka (se necessario)
5. Auto Loader (ja temos CDC, seria complementar)

### Melhorias de Qualidade
1. Great Expectations para DQ
2. Data Lineage completo
3. Alertas detalhados

### Melhorias de Performance
1. Z-Order por regiao/data
2. Particionamento otimizado
3. Caching de dimensoes

---

## Checklist Pre-Apresentacao

- [x] Analise exploratoria dos dados
- [x] Identificacao de problemas (UTF-16, espacos)
- [x] Arquitetura completa (5 camadas)
- [x] Terraform implementado
- [x] Notebooks implementados
- [x] As 3 queries de negocio implementadas
- [x] Documentacao atualizada
- [x] Governanca (OpenMetadata)
- [x] CDC (PK + row_hash)
- [x] CI/CD com GitHub Actions
- [x] Apache Airflow para orquestracao
- [x] ProcessControl para rastreabilidade
- [x] Testes unitarios
- [ ] Teste de execucao end-to-end (requer Java 11/17)

---

## Notas Tecnicas

### Requisitos para Execucao Local

- Python 3.10+
- Java 11 ou 17 (Java 24 NAO e compativel com PySpark 4.0)
- Poetry para gerenciamento de dependencias

### Comandos Uteis

```bash
# Instalar dependencias
poetry install

# Executar testes (requer Java 11/17)
poetry run pytest tests/ -v

# Executar pipeline
poetry run python notebooks/01_bronze_ingestion.py
```

---

Ultima atualizacao: 02/12/2024
