# Arquitetura - ABInBev Beverage Sales Analytics

## 1. Visao Geral

Este documento descreve a arquitetura tecnica da plataforma de analytics de vendas de bebidas, implementada na **Azure** utilizando a **Arquitetura Medallion** com **Delta Lake**.

**Stack Tecnologico**:
- Cloud: Microsoft Azure (portavel para outras clouds)
- Processamento: Azure HDInsight (Apache Spark)
- Orquestracao: Azure Data Factory
- Armazenamento: Azure Data Lake Storage Gen2
- Formato: Delta Lake
- Infraestrutura como Codigo: Terraform
- Observabilidade: Prometheus + Grafana + Loki (open source)
- Governanca: OpenMetadata (open source)

**Modo de Processamento**: Batch (nao ha requisito de real-time no case)

---

## 2. Decisoes de Arquitetura

### 2.1 Por que Azure HDInsight (e nao Databricks)?

| Criterio | HDInsight | Databricks |
|----------|-----------|------------|
| Portabilidade | Cluster Spark padrao, facil migrar para GCP Dataproc ou AWS EMR | Proprietario, lock-in |
| Custo | Pay-per-use, sem licenca adicional | Licenca Databricks adicional |
| Autoscaling | Suporte nativo com configuracao de threshold | Suporte nativo |
| Compatibilidade | Spark open-source padrao | Spark otimizado (Photon) |

**Decisao**: HDInsight para manter portabilidade entre clouds. Se futuramente a empresa optar por Databricks, a migracao sera simples pois o codigo Spark e compativel.

### 2.2 Por que Azure Data Factory para Orquestracao?

| Criterio | Justificativa |
|----------|---------------|
| Integracao nativa | Conecta diretamente com HDInsight e Data Lake |
| Agendamento | Triggers por tempo (cron) ou evento (arquivo chegou) |
| Sequenciamento | Pipelines com dependencias entre atividades |
| Monitoramento | Logs, alertas e retry automatico |
| Low-code | Interface visual para pipelines, reduz complexidade |

**Alternativa considerada**: Apache Airflow. Foi descartado para MVP por adicionar complexidade de infraestrutura (AKS ou Container Instances).

### 2.3 Por que Delta Lake?

| Criterio | Justificativa |
|----------|---------------|
| ACID Transactions | Garante consistencia em operacoes concorrentes |
| Time Travel | Permite consultar versoes anteriores dos dados |
| Schema Evolution | Suporta alteracoes de schema sem reprocessamento |
| Compatibilidade | Funciona com Spark padrao (HDInsight) |
| Merge/Upsert | Operacoes incrementais eficientes |

### 2.4 Por que Arquitetura Medallion Estendida?

| Criterio | Justificativa |
|----------|---------------|
| Separacao de responsabilidades | Cada camada tem proposito claro |
| Reprocessamento | Possivel reprocessar a partir de qualquer camada |
| Qualidade progressiva | Dados ficam mais refinados a cada camada |
| Debugging | Facilita identificar onde ocorreu erro |
| Governanca | Controle de acesso por camada |

**Camadas definidas**:
- Landing: Arquivos brutos
- Bronze: Schema-on-read + auditoria + PK + row_hash (historico completo)
- Silver: Data Quality + limpeza + UPSERT incremental (dado mais atual)
- Gold: Regras de negocio + enriquecimento
- Consumption: Modelo dimensional (fatos + dimensoes)

### 2.8 Por que PK + row_hash para CDC?

| Criterio | Justificativa |
|----------|---------------|
| Identificacao unica | PK (MD5) identifica univocamente cada registro |
| Deteccao de mudancas | row_hash (SHA256) detecta alteracoes em qualquer coluna |
| Performance | Comparar hashes e mais rapido que comparar todas as colunas |
| Historico | Bronze mantem todas as versoes (append) |
| Atualidade | Silver mantem apenas o dado mais recente (upsert) |
| Auditoria | _updated_at permite resolver conflitos de versao |

**Fluxo CDC**:
```
Bronze (historico)          Silver (atual)
+----------------+          +----------------+
| PK | hash | v1 |   --->   | PK | hash | v2 |
| PK | hash | v2 |          +----------------+
+----------------+          (mantém mais recente)
```

**Logica de Merge**:
1. Se PK nao existe no Silver: INSERT
2. Se PK existe e hash diferente: UPDATE (se _updated_at mais recente)
3. Se PK existe e hash igual: SKIP (sem mudanca)

### 2.5 Por que Prometheus + Grafana + Loki para Observabilidade?

| Criterio | Justificativa |
|----------|---------------|
| Open Source | Sem vendor lock-in, funciona em qualquer cloud |
| Portabilidade | Mesma stack em Azure, GCP, AWS ou on-premise |
| Comunidade | Ampla documentacao e suporte da comunidade |
| Integracao | Grafana unifica metricas, logs e alertas |
| Custo | Sem licenciamento adicional |

**Componentes**:
- **Prometheus**: Coleta e armazena metricas time-series
- **Grafana**: Dashboards, visualizacao e alertas
- **Loki**: Agregacao de logs (similar ao CloudWatch Logs ou Log Analytics)
- **AlertManager**: Gerenciamento de alertas (integrado ao Prometheus)

### 2.6 Por que Batch e nao Real-Time?

| Evidencia no Case | Conclusao |
|-------------------|-----------|
| Dados sao arquivos CSV estaticos | Batch |
| Periodo dos dados: 2006 (historico) | Batch |
| Objetivo: "Analytics platform" | Batch |
| Nenhuma mencao a streaming | Batch |
| Foco em KPIs e queries analiticas | Batch |

**Evolucao futura**: Auto Loader para ingestao incremental, Kafka para streaming.

### 2.7 Por que OpenMetadata para Governanca?

| Criterio | OpenMetadata | Unity Catalog | Apache Atlas |
|----------|--------------|---------------|--------------|
| Open Source | Sim (Apache 2.0) | Nao (Databricks) | Sim (Apache) |
| Portabilidade | Alta | Baixa (lock-in) | Media |
| Interface | Moderna, intuitiva | Integrada ao Databricks | Antiga |
| Lineage | Automatico + manual | Automatico | Manual |
| Data Quality | Integrado | Parcial | Nao |
| Comunidade | Crescente | N/A | Madura |

**Decisao**: OpenMetadata por ser open source, moderno e portavel entre clouds.

**Funcionalidades**:
- Data Catalog: Descoberta e documentacao de tabelas
- Data Lineage: Rastreamento de transformacoes
- Data Quality: Testes e metricas integrados
- Access Policies: Controle de acesso por camada
- Business Glossary: Termos e definicoes padronizadas
- Alertas: Notificacoes de mudancas e problemas

---

## 3. Diagrama de Arquitetura

```
+-----------------------------------------------------------------------------------+
|                                    AZURE CLOUD                                     |
+-----------------------------------------------------------------------------------+
|                                                                                    |
|  +------------------+     +------------------+     +------------------+            |
|  |    TERRAFORM     |---->|  RESOURCE GROUP  |---->|   DEPLOYMENT     |            |
|  | Infrastructure   |     |  abinbev-case-rg |     |   Automatizado   |            |
|  +------------------+     +------------------+     +------------------+            |
|                                                                                    |
|  +-----------------------------------------------------------------------------+  |
|  |                     AZURE DATA LAKE STORAGE GEN2                             |  |
|  |                     (abinbevdatalake)                                        |  |
|  |                                                                              |  |
|  |  +--------+   +--------+   +--------+   +--------+   +-----------+           |  |
|  |  |LANDING |-->| BRONZE |-->| SILVER |-->|  GOLD  |-->|CONSUMPTION|           |  |
|  |  | (raw)  |   |(delta) |   |(delta) |   |(delta) |   |  (delta)  |           |  |
|  |  +--------+   +--------+   +--------+   +--------+   +-----------+           |  |
|  |                                                            |                 |  |
|  |                                                            v                 |  |
|  |                                                     +-----------+            |  |
|  |                                                     |    BI     |            |  |
|  |                                                     | Dashboards|            |  |
|  |                                                     +-----------+            |  |
|  |                                                                              |  |
|  |  +-------------------+    +-------------------+                              |  |
|  |  | QUARANTINE        |    | CONTROL           |                              |  |
|  |  | (delta)           |    | (delta)           |                              |  |
|  |  +-------------------+    +-------------------+                              |  |
|  |                                                                              |  |
|  +-----------------------------------------------------------------------------+  |
|                                                                                    |
|  +-----------------------------------------------------------------------------+  |
|  |                         AZURE HDINSIGHT                                      |  |
|  |                         (Spark Cluster)                                      |  |
|  |                                                                              |  |
|  |  +------------------+    +------------------+    +------------------+         |  |
|  |  |   Head Nodes     |    |  Worker Nodes    |    |   Autoscaling    |         |  |
|  |  |   (2 nodes)      |    |  (min: 2)        |    |   (max: 10)      |         |  |
|  |  +------------------+    +------------------+    +------------------+         |  |
|  |                                                                              |  |
|  |  Trigger: CPU > 80% -> Scale Up                                              |  |
|  |  Trigger: CPU < 20% -> Scale Down                                            |  |
|  |                                                                              |  |
|  +-----------------------------------------------------------------------------+  |
|                                                                                    |
|  +-----------------------------------------------------------------------------+  |
|  |                      AZURE DATA FACTORY                                      |  |
|  |                      (Orquestracao Sequencial)                               |  |
|  |                                                                              |  |
|  |  Pipeline: pipeline_beverage_analytics                                       |  |
|  |                                                                              |  |
|  |  +--------+   +--------+   +--------+   +-----------+                        |  |
|  |  | BRONZE |-->| SILVER |-->|  GOLD  |-->|CONSUMPTION|                        |  |
|  |  +--------+   +--------+   +--------+   +-----------+                        |  |
|  |      ^                                                                       |  |
|  |      |                                                                       |  |
|  |  Schedule: Daily 02:00 UTC (apenas Bronze e agendado)                        |  |
|  |  Dependencia: Cada etapa so inicia apos anterior terminar com SUCCESS        |  |
|  |  Retry: 3x | Timeout: 2h por etapa                                           |  |
|  |                                                                              |  |
|  +-----------------------------------------------------------------------------+  |
|                                                                                    |
|  +-----------------------------------------------------------------------------+  |
|  |                      OBSERVABILITY STACK (Open Source)                       |  |
|  |                                                                              |  |
|  |  +------------+    +------------+    +------------+    +------------+        |  |
|  |  | PROMETHEUS |    |   LOKI     |    |  GRAFANA   |    | ALERTMGR   |        |  |
|  |  | (metricas) |    |   (logs)   |    | (dashboard)|    | (alertas)  |        |  |
|  |  +------------+    +------------+    +------------+    +------------+        |  |
|  |                                                                              |  |
|  |  Deployment: Azure Container Instances ou AKS                                |  |
|  |                                                                              |  |
|  +-----------------------------------------------------------------------------+  |
|                                                                                    |
+-----------------------------------------------------------------------------------+
```

---

## 4. Detalhamento das Camadas

### 4.1 Landing Zone

**Proposito**: Armazenar arquivos brutos exatamente como recebidos da fonte.

| Caracteristica | Valor |
|----------------|-------|
| Formato | CSV (original, sem transformacao) |
| Estrutura | /landing/{source_name}/{YYYY}/{MM}/{DD}/ |
| Retencao | 90 dias (configuravel) |
| Transformacao | Nenhuma |

**Justificativa**: Permite reprocessamento completo a partir dos dados originais em caso de erro ou mudanca de regra.

**Estrutura de diretorios**:
```
landing/
  beverage_sales/
    2024/12/01/
      abi_bus_case1_beverage_sales_20210726.csv
  channel_features/
    2024/12/01/
      abi_bus_case1_beverage_channel_group_20210726.csv
```

---

### 4.2 Bronze Layer

**Proposito**: Primeira camada persistida em Delta Lake com Schema-on-read e campos de auditoria.

| Caracteristica | Valor |
|----------------|-------|
| Formato | Delta Lake |
| Schema | Schema-on-read (inferido do CSV) |
| Particionamento | ingestion_date |
| Transformacoes | Apenas adicao de campos de auditoria e padronizacao de nomes |

**Campos de Auditoria Adicionados**:

| Campo | Tipo | Descricao |
|-------|------|-----------|
| _ingestion_timestamp | TIMESTAMP | Data/hora UTC da ingestao |
| _source_file | STRING | Path completo do arquivo fonte |
| _batch_id | STRING | Identificador unico do lote (UUID) |
| _ingestion_date | DATE | Data da ingestao (para particionamento) |

**Padronizacao de Nomes de Colunas**:

| Regra | Exemplo |
|-------|---------|
| Lowercase | BRAND_NM -> brand_nm |
| Snake_case | Btlr_Org_LVL_C_Desc -> btlr_org_lvl_c_desc |
| Remover caracteres especiais | $ Volume -> dollar_volume |
| Sem espacos | Pkg Cat Desc -> pkg_cat_desc |

**Justificativa da Padronizacao na Bronze**: 
- Facilita queries SQL (nomes sensiveis a case)
- Evita erros de referencia por case
- Padrao de engenharia de dados (snake_case lowercase)
- Nao altera os VALORES, apenas os NOMES das colunas

**Tabelas**:

| Tabela | Origem | Registros |
|--------|--------|-----------|
| bronze_beverage_sales | abi_bus_case1_beverage_sales_20210726.csv | 16.151 |
| bronze_channel_features | abi_bus_case1_beverage_channel_group_20210726.csv | 31 |

---

### 4.3 Silver Layer

**Proposito**: Camada de Data Quality, limpeza, tipagem e enriquecimento tecnico.

| Caracteristica | Valor |
|----------------|-------|
| Formato | Delta Lake |
| Schema | Schema-on-write (tipos definidos) |
| Particionamento | year, month |
| Transformacoes | Limpeza, tipagem, validacao, enriquecimento (JOIN) |

**Componentes da Silver Layer**:

| Componente | Proposito |
|------------|-----------|
| Tabelas Silver | Dados limpos, tipados e validados |
| Tabela Quarantine | Linhas com erros para analise e reprocessamento |
| Tabela Process Control | Metadados de execucao de todas as camadas |

---

#### 4.3.1 Tabela de Controle de Processos (process_control)

Registra cada execucao de cada tabela em cada camada. Conectada ao Grafana para visualizacao.

| Campo | Tipo | Descricao |
|-------|------|-----------|
| process_id | STRING | UUID unico da execucao |
| batch_id | STRING | ID do lote (agrupa execucoes do mesmo run) |
| layer | STRING | Camada: landing, bronze, silver, gold, consumption |
| table_name | STRING | Nome da tabela processada |
| status | STRING | RUNNING, SUCCESS, FAILED, PARTIAL |
| start_timestamp | TIMESTAMP | Inicio do processamento |
| end_timestamp | TIMESTAMP | Fim do processamento |
| duration_seconds | DECIMAL | Tempo de execucao em segundos |
| records_read | BIGINT | Registros lidos |
| records_written | BIGINT | Registros escritos |
| records_quarantined | BIGINT | Registros enviados para quarentena |
| records_failed | BIGINT | Registros com erro |
| error_message | STRING | Mensagem de erro (se houver) |
| error_stack_trace | STRING | Stack trace completo (se houver) |
| spark_app_id | STRING | ID da aplicacao Spark |
| cluster_id | STRING | ID do cluster HDInsight |
| created_at | TIMESTAMP | Data de criacao do registro |

---

#### 4.3.2 Tabela de Quarentena (quarantine)

Registra linhas que falharam nas validacoes de Data Quality.

| Campo | Tipo | Descricao |
|-------|------|-----------|
| quarantine_id | STRING | UUID unico do registro |
| batch_id | STRING | ID do lote de origem |
| source_table | STRING | Tabela de origem (ex: bronze_beverage_sales) |
| target_table | STRING | Tabela de destino (ex: silver_beverage_sales) |
| record_data | STRING | Linha completa em JSON |
| error_type | STRING | Tipo: VALIDATION_ERROR, UNKNOWN_ERROR |
| error_code | STRING | Codigo do erro (ex: DQ_SALES_001) |
| error_description | STRING | Descricao do erro |
| dq_rule_name | STRING | Nome da regra que falhou (NULL se desconhecido) |
| is_known_rule | BOOLEAN | True se erro conhecido, False se novo erro |
| reprocessed | BOOLEAN | True se ja foi reprocessado |
| reprocess_batch_id | STRING | Batch do reprocessamento (NULL se nao reprocessado) |
| created_at | TIMESTAMP | Data da quarentena |
| updated_at | TIMESTAMP | Ultima atualizacao |

**Fluxo de Quarentena**:
1. Linha falha em validacao conhecida -> Quarentena com is_known_rule = True
2. Linha falha por erro desconhecido -> Quarentena com is_known_rule = False + ALERTA
3. Analista cria nova regra de DQ
4. Reprocessamento busca linhas com reprocessed = False
5. Apos reprocessar, atualiza reprocessed = True

---

#### 4.3.3 Regras de Data Quality

**beverage_sales - Regras Especificas**:

| Codigo | Regra | Campo | Descricao | Acao |
|--------|-------|-------|-----------|------|
| DQ_SALES_001 | NOT_NULL | brand_nm | Marca nao pode ser nula | Quarentena |
| DQ_SALES_002 | NOT_NULL | trade_chnl_desc | Canal nao pode ser nulo | Quarentena |
| DQ_SALES_003 | IS_NUMERIC | dollar_volume | Volume deve ser numerico | Quarentena |
| DQ_SALES_004 | DATE_FORMAT | date | Data deve ser M/D/YYYY | Quarentena |
| DQ_SALES_005 | TRIM_WHITESPACE | brand_nm | Remover espacos inicio/fim | Corrigir |
| DQ_SALES_006 | VALID_YEAR | year | Ano deve estar entre 2000-2030 | Quarentena |
| DQ_SALES_007 | VALID_MONTH | month | Mes deve estar entre 1-12 | Quarentena |
| DQ_SALES_008 | REFERENTIAL | trade_chnl_desc | Deve existir em channel_features | Quarentena |

**channel_features - Regras Especificas**:

| Codigo | Regra | Campo | Descricao | Acao |
|--------|-------|-------|-----------|------|
| DQ_CHAN_001 | NOT_NULL | trade_chnl_desc | Canal nao pode ser nulo | Quarentena |
| DQ_CHAN_002 | NOT_NULL | trade_group_desc | Grupo nao pode ser nulo | Quarentena |
| DQ_CHAN_003 | UNIQUE | trade_chnl_desc | Canal deve ser unico (PK) | Quarentena |
| DQ_CHAN_004 | VALID_VALUES | trade_type_desc | Deve ser ALCOHOLIC, NON ALCOHOLIC ou MIX | Quarentena |

---

#### 4.3.4 Tabelas Silver

| Tabela | Descricao |
|--------|-----------|
| silver_beverage_sales | Vendas limpas e tipadas |
| silver_channel_features | Canais limpos |
| silver_sales_enriched | Vendas + info de canal (JOIN tecnico) |

**Transformacoes aplicadas em silver_beverage_sales**:

| Campo Origem | Campo Destino | Transformacao |
|--------------|---------------|---------------|
| date | transaction_date | to_date(..., "M/d/yyyy") |
| brand_nm | brand_nm | TRIM() |
| dollar_volume | dollar_volume | CAST to DECIMAL(18,2) |
| year | year | CAST to INT |
| month | month | CAST to INT |

---

### 4.4 Gold Layer

**Proposito**: Camada de regras de negocio, enriquecimento de dados e padronizacoes finais.

| Caracteristica | Valor |
|----------------|-------|
| Formato | Delta Lake |
| Schema | Schema-on-write (tipos definidos) |
| Particionamento | year, month |
| Transformacoes | Regras de negocio, campos calculados, categorizacoes |

**Diferencas entre Silver e Gold**:

| Aspecto | Silver | Gold |
|---------|--------|------|
| Foco | Qualidade tecnica | Regras de negocio |
| Transformacoes | Limpeza, tipagem | Calculos, categorizacoes |
| Responsavel | Engenharia de Dados | Engenharia + Negocio |
| Consumidor | Camada Gold | Camada Consumption |

---

#### 4.4.1 Regras de Negocio Aplicadas

| Campo Calculado | Logica | Justificativa |
|-----------------|--------|---------------|
| volume_category | HIGH (>1000), MEDIUM (>100), LOW (<=100) | Classificacao para segmentacao de clientes |
| is_negative_sale | True se dollar_volume < 0 | Identificar devolucoes/estornos |
| region_group | US_EAST, US_WEST, CANADA | Agrupamento geografico simplificado |
| brand_category | Calculado por performance | Classificacao para analise de portfolio |
| sales_quarter | Q1, Q2, Q3, Q4 | Analise trimestral |
| is_weekend_sale | True se dia = sabado/domingo | Analise de comportamento |

**Mapeamento region_group**:

| Regiao Original | region_group |
|-----------------|--------------|
| NORTHEAST | US_EAST |
| SOUTHEAST | US_EAST |
| GREAT LAKES | US_EAST |
| MIDWEST | US_WEST |
| SOUTHWEST | US_WEST |
| WEST | US_WEST |
| CANADA | CANADA |

---

#### 4.4.2 Tabelas Gold

| Tabela | Descricao |
|--------|-----------|
| gold_sales_enriched | Vendas com todas as regras de negocio aplicadas |
| gold_channels_enriched | Canais com categorizacoes de negocio |

**Campos adicionais em gold_sales_enriched**:

| Campo | Tipo | Descricao |
|-------|------|-----------|
| volume_category | STRING | HIGH, MEDIUM, LOW |
| is_negative_sale | BOOLEAN | Indica devolucao/estorno |
| region_group | STRING | US_EAST, US_WEST, CANADA |
| sales_quarter | STRING | Q1, Q2, Q3, Q4 |
| is_weekend_sale | BOOLEAN | Venda em fim de semana |
| _gold_timestamp | TIMESTAMP | Timestamp do processamento Gold |

---

### 4.5 Consumption Layer

**Proposito**: Modelagem dimensional para conexao com BI e areas de negocio.

| Caracteristica | Valor |
|----------------|-------|
| Formato | Delta Lake |
| Modelo | Star Schema (Fatos + Dimensoes) |
| Particionamento | Variavel por tabela |
| Consumidores | Dashboards, Power BI, Areas de Negocio |

**Justificativa do nome "Consumption"**: Indica claramente que e a camada final de consumo pelos usuarios de negocio.

---

#### 4.5.1 Dimensoes

**dim_date**:

| Campo | Tipo | Descricao |
|-------|------|-----------|
| date_key | INT | PK (formato YYYYMMDD) |
| full_date | DATE | Data completa |
| year | INT | Ano |
| quarter | INT | Trimestre (1-4) |
| quarter_name | STRING | Q1, Q2, Q3, Q4 |
| month | INT | Mes (1-12) |
| month_name | STRING | Nome do mes |
| week_of_year | INT | Semana do ano |
| day_of_month | INT | Dia do mes |
| day_of_week | INT | Dia da semana (1=Domingo) |
| day_name | STRING | Nome do dia |
| is_weekend | BOOLEAN | Flag de fim de semana |
| year_month | STRING | Ano-Mes (YYYY-MM) |

**dim_region**:

| Campo | Tipo | Descricao |
|-------|------|-----------|
| region_key | INT | PK (surrogate) |
| region_name | STRING | Nome original (CANADA, MIDWEST, etc) |
| region_group | STRING | Agrupamento (US_EAST, US_WEST, CANADA) |
| country | STRING | Pais |

**dim_product**:

| Campo | Tipo | Descricao |
|-------|------|-----------|
| product_key | INT | PK (surrogate) |
| ce_brand_flvr | STRING | Codigo do produto/sabor |
| brand_nm | STRING | Nome da marca |
| pkg_cat | STRING | Categoria de embalagem |
| pkg_cat_desc | STRING | Descricao da categoria |
| tsr_pckg_nm | STRING | Nome do pacote |

**dim_channel**:

| Campo | Tipo | Descricao |
|-------|------|-----------|
| channel_key | INT | PK (surrogate) |
| chnl_group | STRING | Grupo de canal |
| trade_chnl_desc | STRING | Tipo de canal |
| trade_group_desc | STRING | Grupo de trade |
| trade_type_desc | STRING | Tipo de produto (ALCOHOLIC, etc) |

---

#### 4.5.2 Tabela Fato

**fact_sales**:

| Campo | Tipo | Descricao |
|-------|------|-----------|
| date_key | INT | FK -> dim_date |
| region_key | INT | FK -> dim_region |
| product_key | INT | FK -> dim_product |
| channel_key | INT | FK -> dim_channel |
| dollar_volume | DECIMAL(18,2) | Volume em dolares |
| volume_category | STRING | HIGH, MEDIUM, LOW |
| is_negative_sale | BOOLEAN | Indica devolucao/estorno |
| _consumption_timestamp | TIMESTAMP | Timestamp do processamento |

**Grain**: Uma linha por combinacao unica de date/region/product/channel.

---

#### 4.5.3 Tabelas Agregadas

Tabelas pre-calculadas para responder as perguntas de negocio do case.

**agg_sales_region_tradegroup** (Responde pergunta 4.1):

| Campo | Tipo | Descricao |
|-------|------|-----------|
| region_name | STRING | Regiao |
| trade_group_desc | STRING | Grupo de trade |
| total_dollar_volume | DECIMAL(18,2) | Soma de vendas |
| transaction_count | BIGINT | Quantidade de transacoes |
| rank | INT | Ranking (1 = maior volume) |

**agg_sales_brand_month** (Responde pergunta 4.2):

| Campo | Tipo | Descricao |
|-------|------|-----------|
| brand_nm | STRING | Nome da marca |
| year | INT | Ano |
| month | INT | Mes |
| year_month | STRING | Ano-Mes |
| total_dollar_volume | DECIMAL(18,2) | Soma de vendas |

**agg_sales_brand_region** (Responde pergunta 4.3):

| Campo | Tipo | Descricao |
|-------|------|-----------|
| region_name | STRING | Regiao |
| brand_nm | STRING | Nome da marca |
| total_dollar_volume | DECIMAL(18,2) | Soma de vendas |
| rank_asc | INT | Ranking ascendente (1 = menor venda) |
| rank_desc | INT | Ranking descendente (1 = maior venda) |

---

## 5. Orquestracao Sequencial

### 5.1 Fluxo de Execucao

```
+----------+     +----------+     +----------+     +-------------+
|  BRONZE  |---->|  SILVER  |---->|   GOLD   |---->| CONSUMPTION |
+----------+     +----------+     +----------+     +-------------+
     ^
     |
 Schedule
 Daily 02:00 UTC
```

**Regras de Dependencia**:

| Etapa | Depende de | Condicao |
|-------|------------|----------|
| Bronze | Schedule (02:00 UTC) | Trigger automatico |
| Silver | Bronze | Bronze.status = SUCCESS |
| Gold | Silver | Silver.status = SUCCESS |
| Consumption | Gold | Gold.status = SUCCESS |

### 5.2 Configuracao no Azure Data Factory

```json
{
  "pipeline": "pipeline_beverage_analytics",
  "activities": [
    {
      "name": "01_bronze_ingestion",
      "type": "HDInsightSpark",
      "dependsOn": [],
      "policy": {
        "timeout": "02:00:00",
        "retry": 3,
        "retryIntervalInSeconds": 300
      }
    },
    {
      "name": "02_silver_transformation",
      "type": "HDInsightSpark",
      "dependsOn": [
        {
          "activity": "01_bronze_ingestion",
          "dependencyConditions": ["Succeeded"]
        }
      ]
    },
    {
      "name": "03_gold_business_rules",
      "type": "HDInsightSpark",
      "dependsOn": [
        {
          "activity": "02_silver_transformation",
          "dependencyConditions": ["Succeeded"]
        }
      ]
    },
    {
      "name": "04_consumption_dimensional",
      "type": "HDInsightSpark",
      "dependsOn": [
        {
          "activity": "03_gold_business_rules",
          "dependencyConditions": ["Succeeded"]
        }
      ]
    }
  ],
  "trigger": {
    "type": "ScheduleTrigger",
    "recurrence": {
      "frequency": "Day",
      "interval": 1,
      "startTime": "2024-12-01T02:00:00Z",
      "timeZone": "UTC"
    }
  }
}
```

---

## 6. Observabilidade (Open Source Stack)

### 6.1 Componentes

| Componente | Funcao | Deployment |
|------------|--------|------------|
| Prometheus | Coleta e armazena metricas time-series | ACI ou AKS |
| Grafana | Dashboards, visualizacao, alertas | ACI ou AKS |
| Loki | Agregacao de logs estruturados | ACI ou AKS |
| AlertManager | Gerenciamento e roteamento de alertas | Integrado ao Prometheus |

### 6.2 Metricas Exportadas para Prometheus

| Metrica | Tipo | Labels | Descricao |
|---------|------|--------|-----------|
| pipeline_duration_seconds | Gauge | layer, table | Tempo de processamento |
| pipeline_records_processed | Counter | layer, table | Total de registros processados |
| pipeline_records_quarantined | Counter | layer, table | Total de registros em quarentena |
| pipeline_errors_total | Counter | layer, table, error_type | Total de erros |
| pipeline_unknown_errors | Counter | layer, table | Erros desconhecidos (trigger alerta) |
| pipeline_status | Gauge | layer, table | 1=running, 2=success, 3=failed |

### 6.3 Logs Estruturados para Loki

Formato JSON para facilitar queries:

```json
{
  "timestamp": "2024-12-01T02:15:30Z",
  "level": "INFO",
  "layer": "silver",
  "table": "silver_beverage_sales",
  "batch_id": "20241201_021500",
  "process_id": "uuid-1234",
  "message": "Data quality validation completed",
  "records_passed": 16100,
  "records_failed": 51,
  "duration_ms": 45230
}
```

### 6.4 Alertas Configurados no Grafana

| Alerta | Condicao | Severidade | Notificacao |
|--------|----------|------------|-------------|
| Pipeline Failed | pipeline_status == 3 | Critical | Email + Slack |
| Unknown DQ Error | pipeline_unknown_errors > 0 | Warning | Slack |
| High Quarantine Rate | quarantine_rate > 5% | Warning | Email |
| Pipeline Slow | duration > 2x media historica | Info | Slack |
| Cluster High CPU | cpu_usage > 90% por 10min | Warning | Email |

### 6.5 Dashboard Grafana

Paineis planejados:

1. **Pipeline Overview**: Status de todos os pipelines em tempo real
2. **Processing Metrics**: Registros processados, quarentenados, falhados por tabela
3. **Duration Trends**: Tempo de processamento ao longo do tempo
4. **Data Quality**: Taxa de erro por regra de DQ
5. **Cluster Health**: CPU, memoria, workers ativos
6. **Alerts History**: Historico de alertas disparados

---

## 7. Governanca de Dados (OpenMetadata)

### 7.1 Visao Geral

OpenMetadata e a plataforma de governanca de dados open source escolhida para:
- Catalogar todas as tabelas do Data Lake
- Rastrear lineage de transformacoes
- Monitorar qualidade de dados
- Definir politicas de acesso
- Manter glossario de termos de negocio

### 7.2 Arquitetura OpenMetadata

```
+-----------------------------------------------------------------------------+
|                         OPENMETADATA STACK                                   |
+-----------------------------------------------------------------------------+
|                                                                              |
|  +------------------+     +------------------+     +------------------+       |
|  |   PostgreSQL     |     |  Elasticsearch   |     |  OpenMetadata    |       |
|  |   (metadata)     |     |   (search)       |     |   (server)       |       |
|  +------------------+     +------------------+     +------------------+       |
|                                                            |                 |
|                                                            v                 |
|                                                   +------------------+       |
|                                                   |   Web UI         |       |
|                                                   |   (port 8585)    |       |
|                                                   +------------------+       |
|                                                                              |
+-----------------------------------------------------------------------------+
```

### 7.3 Politicas de Acesso por Camada

| Camada | Leitura | Escrita |
|--------|---------|---------|
| Landing | Data Engineer, Admin | Data Engineer, Admin |
| Bronze | Data Engineer, Data Scientist, Admin | Data Engineer, Admin |
| Silver | Data Engineer, Data Scientist, Analyst, Admin | Data Engineer, Admin |
| Gold | Data Engineer, Scientist, Analyst, Business, Admin | Data Engineer, Admin |
| Consumption | Todos (BI, Business, Analyst) | Data Engineer, Admin |

### 7.4 Classificacao de Dados

| Nivel | Descricao | Exemplos |
|-------|-----------|----------|
| PUBLIC | Sem restricao | Agregacoes publicas |
| INTERNAL | Acesso interno | Vendas por regiao |
| CONFIDENTIAL | Acesso restrito | Margens de lucro |
| RESTRICTED | Acesso controlado | Dados de clientes |

### 7.5 Lineage Automatico

O lineage e registrado automaticamente em cada etapa do pipeline:

```
bronze_beverage_sales --> silver_sales_enriched --> gold_sales_enriched --> fact_sales
bronze_channel_features --> silver_channel_features --> gold_channels_enriched --> dim_channel
```

### 7.6 Integracao com Notebooks

Os notebooks registram automaticamente:
- Tabelas criadas/atualizadas
- Lineage entre tabelas
- Resultados de Data Quality

Exemplo de uso:

```python
from src.governance import register_pipeline_lineage, report_dq_results

# Registra lineage
register_pipeline_lineage("bronze", "silver", [
    {"source": "bronze_sales", "target": "silver_sales", "desc": "DQ + Limpeza"}
])

# Reporta resultados de DQ
report_dq_results("silver_sales", [
    {"name": "null_check", "type": "completeness", "passed": True, "value": 0.99, "threshold": 0.95}
])
```

### 7.7 Glossario de Negocios

Termos padronizados para toda a organizacao:

| Termo | Definicao |
|-------|-----------|
| Dollar Volume | Valor monetario das vendas em dolares |
| Trade Group | Agrupamento de canais por tipo |
| Region | Divisao geografica de vendas |
| Negative Sale | Devolucao ou estorno |

---

## 8. Infraestrutura (Terraform)

### 7.1 Recursos Provisionados

| Recurso | Nome | Configuracao |
|---------|------|--------------|
| Resource Group | abinbev-case-rg | East US 2 |
| Storage Account | abinbevdatalake | ADLS Gen2, LRS |
| Containers | landing, bronze, silver, gold, consumption, control | Hierarchical namespace |
| HDInsight Spark | abinbev-spark-cluster | Spark 3.3, 2 head + 2-10 workers |
| Data Factory | abinbev-adf | Pipelines + Triggers |
| Container Instances | prometheus, grafana, loki | Observability stack |
| Container Instances | openmetadata, elasticsearch | Governance stack |
| PostgreSQL Flexible | openmetadata-db | Metadata store |

### 7.2 Configuracao do Autoscaling

```hcl
autoscale {
  capacity {
    min_instance_count = 2
    max_instance_count = 10
  }
  recurrence {
    schedule {
      days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
      time_zone = "UTC"
      time = "02:00"
    }
    min_instance_count = 4
    max_instance_count = 10
  }
}
```

**Regras de Scaling**:

| Trigger | Acao |
|---------|------|
| CPU > 80% por 5 min | Adiciona 2 workers |
| CPU < 20% por 10 min | Remove 1 worker |
| Minimo | 2 workers |
| Maximo | 10 workers |

---

## 9. Fluxo de Dados Completo

```
1. LANDING
   - Arquivo chega via upload
   - Sem transformacao

2. BRONZE (agendado 02:00 UTC)
   - Registra em process_control (RUNNING)
   - Le arquivo, infere schema
   - Padroniza nomes (lowercase, snake_case)
   - Adiciona campos de auditoria
   - Salva em Delta
   - Atualiza process_control (SUCCESS/FAILED)
   - Envia metricas para Prometheus

3. SILVER (apos Bronze SUCCESS)
   - Registra em process_control (RUNNING)
   - Le Bronze
   - Aplica regras de Data Quality
   - Registros validos -> Silver
   - Registros invalidos -> Quarantine
   - Enriquece com JOIN (sales + channels)
   - Atualiza process_control (SUCCESS/FAILED)
   - Envia metricas e logs
   - Se erro desconhecido -> ALERTA

4. GOLD (apos Silver SUCCESS)
   - Registra em process_control (RUNNING)
   - Le Silver
   - Aplica regras de negocio
   - Adiciona campos calculados
   - Aplica categorizacoes
   - Salva em Delta
   - Atualiza process_control (SUCCESS/FAILED)

5. CONSUMPTION (apos Gold SUCCESS)
   - Registra em process_control (RUNNING)
   - Gera/atualiza dimensoes
   - Gera surrogate keys
   - Popula fact_sales
   - Calcula agregacoes
   - Atualiza process_control (SUCCESS/FAILED)
   - Dados prontos para BI
```

---

## 10. Estrutura do Projeto

```
abinbev_case/
  infrastructure/
    terraform/
      main.tf
      variables.tf
      outputs.tf
      modules/
        hdinsight/
        data_factory/
        storage/
        observability/       # Prometheus, Grafana, Loki
  notebooks/
    01_bronze_ingestion.py
    02_silver_transformation.py
    03_gold_business_rules.py
    04_consumption_dimensional.py
  src/
    data_quality/
      rules/
        sales_rules.py
        channel_rules.py
      validator.py
      quarantine.py
    business_rules/
      sales_rules.py         # Regras de negocio para Gold
      categorization.py      # Categorizacoes
    control/
      process_control.py
      metrics.py
      logging.py
  config/
    config.yaml
    dq_rules/
      sales.yaml
      channels.yaml
    business_rules/
      sales.yaml             # Configuracao de regras de negocio
  docs/
    ARCHITECTURE.md
    DATA_DICTIONARY.md
    DEVELOPMENT_JOURNAL.md
  tests/
    test_bronze.py
    test_silver.py
    test_gold.py
    test_consumption.py
    test_data_quality.py
```

---

## 11. Registro de Decisoes

| # | Data | Decisao | Motivo |
|---|------|---------|--------|
| 1 | 01/12/2024 | Azure HDInsight em vez de Databricks | Portabilidade entre clouds |
| 2 | 01/12/2024 | Azure Data Factory para orquestracao | Integracao nativa, low-code |
| 3 | 01/12/2024 | Delta Lake como formato | ACID, Time Travel, Schema Evolution |
| 4 | 01/12/2024 | Padronizacao de nomes na Bronze | Padrao de engenharia, evita erros |
| 5 | 01/12/2024 | Terraform para IaC | Reproducibilidade, versionamento |
| 6 | 01/12/2024 | Autoscaling 80% CPU | Otimizacao de custo e performance |
| 7 | 01/12/2024 | Prometheus + Grafana + Loki | Open source, portabilidade entre clouds |
| 8 | 01/12/2024 | Tabela process_control | Rastreabilidade de todos os processos |
| 9 | 01/12/2024 | Tabela quarantine com reprocessamento | Tratamento de erros desconhecidos |
| 10 | 01/12/2024 | Regras DQ especificas por arquivo | Cada fonte tem suas particularidades |
| 11 | 01/12/2024 | Batch processing (nao real-time) | Requisitos do case nao exigem streaming |
| 12 | 01/12/2024 | Camada Gold para regras de negocio | Separar logica tecnica (Silver) de negocio (Gold) |
| 13 | 01/12/2024 | Camada Consumption para dimensional | Nome indica claramente o proposito de consumo |
| 14 | 01/12/2024 | Orquestracao sequencial | Apenas Bronze agendado, demais dependem do anterior |
| 15 | 01/12/2024 | OpenMetadata para governanca | Open source, moderno, portavel entre clouds |
| 16 | 01/12/2024 | Politicas de acesso por camada | Controle granular de quem acessa cada camada |
| 17 | 01/12/2024 | Lineage automatico | Rastreabilidade completa das transformacoes |

---

Ultima atualizacao: 01/12/2024
