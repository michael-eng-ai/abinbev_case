# Dicionario de Dados - ABInBev Beverage Sales Analytics

## 1. Visao Geral

Este documento descreve todas as tabelas, campos e transformacoes aplicadas em cada camada da arquitetura.

**Camadas**:
- Landing: Arquivos brutos
- Bronze: Schema-on-read + auditoria + CDC
- Silver: Data Quality + limpeza + UPSERT
- Gold: Regras de negocio
- Consumption: Modelo dimensional
- Control: Tabelas de controle (process_control, quarantine)

---

## 1.1 Campos de Auditoria e Rastreabilidade

Todos os registros possuem campos de auditoria que permitem rastrear:
- **De onde veio**: `_source_file`, `_source_path`
- **Quando foi ingerido**: `_ingestion_date`, `_ingestion_timestamp`
- **Ultima atualizacao**: `_updated_at` (atualizado em cada camada)
- **Em qual camada esta**: `_layer`

### Fluxo de _updated_at

| Camada | _updated_at |
|--------|-------------|
| Bronze | Timestamp da ingestao |
| Silver | Timestamp do processamento Silver |
| Gold | Timestamp do processamento Gold |
| Consumption | Timestamp do processamento Consumption |

### Exemplo de Rastreabilidade

```
Pergunta: "De onde veio esse registro na fact_sales?"

Resposta:
- _source_file: abi_bus_case1_beverage_sales_20210726.csv
- _ingestion_date: 2024-12-01
- Ultima atualizacao: 2024-12-01 14:30:00 (Gold -> Consumption)
```

---

## 2. Fontes de Dados (Landing)

### 2.1 abi_bus_case1_beverage_sales_20210726.csv

**Descricao**: Dados transacionais de vendas de bebidas
**Encoding**: UTF-16 Little-Endian
**Delimitador**: TAB (\t)
**Registros**: 16.151 (excluindo header)

| Coluna Original | Tipo | Descricao | Exemplo |
|-----------------|------|-----------|---------|
| DATE | STRING | Data da transacao (M/D/YYYY) | 1/1/2006 |
| CE_BRAND_FLVR | STRING | Codigo do produto/sabor | 3440, 3554 |
| BRAND_NM | STRING | Nome da marca (tem espaco no inicio) | " LEMON" |
| Btlr_Org_LVL_C_Desc | STRING | Regiao geografica | CANADA, MIDWEST |
| CHNL_GROUP | STRING | Grupo de canal de vendas | LEISURE, SUPERS |
| TRADE_CHNL_DESC | STRING | Tipo de canal (chave de join) | SPORT VENUE |
| PKG_CAT | STRING | Codigo categoria de embalagem | N20O, N56P |
| Pkg_Cat_Desc | STRING | Descricao categoria embalagem | 20Z/600ML |
| TSR_PCKG_NM | STRING | Nome do pacote | .591L NRP 24L |
| $ Volume | STRING | Volume em dolares | 22.48, -4.22 |
| YEAR | STRING | Ano da transacao | 2006 |
| MONTH | STRING | Mes da transacao (1-12) | 1, 12 |
| PERIOD | STRING | Periodo | 1, 12 |

**Observacoes**:
- BRAND_NM tem espaco no inicio de todos os valores
- $ Volume pode ter valores negativos (devolucoes/estornos)
- Todos os dados sao do ano 2006

---

### 2.2 abi_bus_case1_beverage_channel_group_20210726.csv

**Descricao**: Dados master de canais e grupos de trade
**Encoding**: UTF-8
**Delimitador**: COMMA (,)
**Registros**: 31

| Coluna | Tipo | Descricao | Valores Exemplo |
|--------|------|-----------|-----------------|
| TRADE_CHNL_DESC | STRING (PK) | Tipo de canal de vendas | SPORT VENUE, SUPERETTE |
| TRADE_GROUP_DESC | STRING | Grupo do trade | ENTERTAINMENT, GROCERY |
| TRADE_TYPE_DESC | STRING | Tipo de produto vendido | ALCOHOLIC, NON ALCOHOLIC, MIX |

**Valores de TRADE_GROUP_DESC** (6):
- ENTERTAINMENT
- GROCERY
- SERVICES
- OTHER
- GOV & MILITARY
- ACADEMIC

**Valores de TRADE_TYPE_DESC** (3):
- ALCOHOLIC
- NON ALCOHOLIC
- MIX

---

## 3. Bronze Layer

**Caracteristicas da Bronze**:
- Mantem historico completo (append)
- Campos de CDC para processamento incremental
- PK gerada como hash MD5 das colunas de identificacao
- row_hash gerado como SHA256 de todas as colunas de negocio

### 3.1 bronze_beverage_sales

**Proposito**: Dados brutos de vendas com campos de auditoria e CDC
**Formato**: Delta Lake
**Particionamento**: _ingestion_date
**Modo de escrita**: APPEND (historico completo)
**PK**: ce_brand_flvr + region + trade_chnl_desc + year + month

| Campo | Tipo | Origem | Transformacao |
|-------|------|--------|---------------|
| **_pk** | STRING | - | MD5(ce_brand_flvr\|region\|trade_chnl_desc\|year\|month) |
| **_row_hash** | STRING | - | SHA256(todas colunas de negocio) |
| **_source_file** | STRING | - | Nome do arquivo fonte (rastreabilidade) |
| **_source_path** | STRING | - | Path completo do arquivo |
| **_ingestion_date** | DATE | - | Data da ingestao (quando foi ingerido) |
| **_ingestion_timestamp** | TIMESTAMP | - | Timestamp exato da ingestao |
| **_updated_at** | TIMESTAMP | - | Atualizado em cada passagem de camada |
| **_batch_id** | STRING | - | UUID do lote |
| **_layer** | STRING | - | Camada atual (bronze) |
| date | STRING | DATE | Renomeado para lowercase |
| ce_brand_flvr | STRING | CE_BRAND_FLVR | Renomeado para snake_case |
| brand_nm | STRING | BRAND_NM | Renomeado para lowercase |
| btlr_org_lvl_c_desc | STRING | Btlr_Org_LVL_C_Desc | Renomeado para snake_case |
| chnl_group | STRING | CHNL_GROUP | Renomeado para lowercase |
| trade_chnl_desc | STRING | TRADE_CHNL_DESC | Renomeado para lowercase |
| pkg_cat | STRING | PKG_CAT | Renomeado para lowercase |
| pkg_cat_desc | STRING | Pkg_Cat_Desc | Renomeado para snake_case |
| tsr_pckg_nm | STRING | TSR_PCKG_NM | Renomeado para lowercase |
| dollar_volume | STRING | $ Volume | Renomeado, removido $ |
| year | STRING | YEAR | Renomeado para lowercase |
| month | STRING | MONTH | Renomeado para lowercase |
| period | STRING | PERIOD | Renomeado para lowercase |

---

### 3.2 bronze_channel_features

**Proposito**: Dados brutos de canais com campos de auditoria e CDC
**Formato**: Delta Lake
**Particionamento**: _ingestion_date
**Modo de escrita**: APPEND (historico completo)
**PK**: trade_chnl_desc

| Campo | Tipo | Origem | Transformacao |
|-------|------|--------|---------------|
| **_pk** | STRING | - | MD5(trade_chnl_desc) |
| **_row_hash** | STRING | - | SHA256(todas colunas de negocio) |
| **_source_file** | STRING | - | Nome do arquivo fonte (rastreabilidade) |
| **_source_path** | STRING | - | Path completo do arquivo |
| **_ingestion_date** | DATE | - | Data da ingestao |
| **_ingestion_timestamp** | TIMESTAMP | - | Timestamp exato da ingestao |
| **_updated_at** | TIMESTAMP | - | Atualizado em cada camada |
| **_batch_id** | STRING | - | UUID do lote |
| **_layer** | STRING | - | Camada atual (bronze) |
| trade_chnl_desc | STRING | TRADE_CHNL_DESC | Renomeado para lowercase |
| trade_group_desc | STRING | TRADE_GROUP_DESC | Renomeado para lowercase |
| trade_type_desc | STRING | TRADE_TYPE_DESC | Renomeado para lowercase |

---

## 4. Silver Layer

**Caracteristicas da Silver**:
- Mantem dado mais atual (UPSERT)
- Merge incremental usando PK + row_hash
- Logica: INSERT novos, UPDATE alterados (se mais recente), SKIP inalterados

**Logica de Merge**:
```
1. Se _pk NAO existe no Silver: INSERT
2. Se _pk existe E _row_hash diferente E _updated_at >= Silver: UPDATE
3. Se _pk existe E _row_hash igual: SKIP (sem mudanca)
4. Se _pk existe E _updated_at < Silver: SKIP (dado antigo)
```

### 4.1 silver_beverage_sales

**Proposito**: Vendas limpas, tipadas e validadas (dado mais atual)
**Formato**: Delta Lake
**Particionamento**: year, month
**Modo de escrita**: OVERWRITE (apos merge incremental)

| Campo | Tipo | Origem | Transformacao |
|-------|------|--------|---------------|
| **_pk** | STRING | _pk | Herdado do Bronze |
| **_row_hash** | STRING | _row_hash | Herdado do Bronze |
| **_source_file** | STRING | _source_file | Herdado (rastreabilidade) |
| **_source_path** | STRING | _source_path | Herdado (rastreabilidade) |
| **_ingestion_date** | DATE | _ingestion_date | Herdado (quando foi ingerido) |
| **_updated_at** | TIMESTAMP | - | ATUALIZADO para Silver timestamp |
| **_layer** | STRING | - | ATUALIZADO para "silver" |
| **_silver_timestamp** | TIMESTAMP | - | Timestamp do processamento Silver |
| sales_id | BIGINT | - | monotonically_increasing_id() |
| transaction_date | DATE | date | to_date(..., "M/d/yyyy") |
| year | INT | year | CAST to INT |
| month | INT | month | CAST to INT |
| period | INT | period | CAST to INT |
| year_month | STRING | - | concat(year, "-", lpad(month, 2, "0")) |
| ce_brand_flvr | STRING | ce_brand_flvr | - |
| brand_nm | STRING | brand_nm | TRIM() |
| region | STRING | btlr_org_lvl_c_desc | Renomeado + TRIM() |
| chnl_group | STRING | chnl_group | TRIM() |
| trade_chnl_desc | STRING | trade_chnl_desc | TRIM() |
| pkg_cat | STRING | pkg_cat | - |
| pkg_cat_desc | STRING | pkg_cat_desc | - |
| tsr_pckg_nm | STRING | tsr_pckg_nm | - |
| dollar_volume | DECIMAL(18,2) | dollar_volume | CAST to DECIMAL |
| _process_timestamp | TIMESTAMP | - | current_timestamp() |
| _batch_id | STRING | _batch_id | Herdado do Bronze |

---

### 4.2 silver_channel_features

**Proposito**: Canais limpos e validados
**Formato**: Delta Lake

| Campo | Tipo | Origem | Transformacao |
|-------|------|--------|---------------|
| channel_sk | BIGINT | - | monotonically_increasing_id() |
| trade_chnl_desc | STRING | trade_chnl_desc | TRIM() |
| trade_group_desc | STRING | trade_group_desc | TRIM() |
| trade_type_desc | STRING | trade_type_desc | TRIM() |
| _process_timestamp | TIMESTAMP | - | current_timestamp() |

---

### 4.3 silver_sales_enriched

**Proposito**: Vendas enriquecidas com dados de canal (JOIN tecnico)
**Formato**: Delta Lake
**Particionamento**: year, month

| Campo | Tipo | Origem | Descricao |
|-------|------|--------|-----------|
| *campos de silver_beverage_sales* | - | silver_beverage_sales | Todos os campos |
| trade_group_desc | STRING | silver_channel_features | Via JOIN |
| trade_type_desc | STRING | silver_channel_features | Via JOIN |

**Chave de JOIN**: trade_chnl_desc

---

## 5. Gold Layer

### 5.1 gold_sales_enriched

**Proposito**: Vendas com regras de negocio aplicadas
**Formato**: Delta Lake
**Particionamento**: year, month

| Campo | Tipo | Origem | Transformacao/Logica |
|-------|------|--------|----------------------|
| *campos de silver_sales_enriched* | - | silver_sales_enriched | Todos os campos |
| volume_category | STRING | dollar_volume | HIGH (>1000), MEDIUM (>100), LOW (<=100) |
| is_negative_sale | BOOLEAN | dollar_volume | True se dollar_volume < 0 |
| region_group | STRING | region | Mapeamento para US_EAST, US_WEST, CANADA |
| sales_quarter | STRING | month | Q1 (1-3), Q2 (4-6), Q3 (7-9), Q4 (10-12) |
| is_weekend_sale | BOOLEAN | transaction_date | True se sabado ou domingo |
| _gold_timestamp | TIMESTAMP | - | current_timestamp() |

**Mapeamento region_group**:

| region | region_group |
|--------|--------------|
| NORTHEAST | US_EAST |
| SOUTHEAST | US_EAST |
| GREAT LAKES | US_EAST |
| MIDWEST | US_WEST |
| SOUTHWEST | US_WEST |
| WEST | US_WEST |
| CANADA | CANADA |

---

### 5.2 gold_channels_enriched

**Proposito**: Canais com categorizacoes de negocio
**Formato**: Delta Lake

| Campo | Tipo | Origem | Transformacao |
|-------|------|--------|---------------|
| *campos de silver_channel_features* | - | silver_channel_features | Todos os campos |
| is_alcoholic_channel | BOOLEAN | trade_type_desc | True se ALCOHOLIC ou MIX |
| channel_category | STRING | trade_group_desc | Categorizacao adicional |
| _gold_timestamp | TIMESTAMP | - | current_timestamp() |

---

## 6. Consumption Layer (Modelo Dimensional)

### 6.1 dim_date

**Proposito**: Dimensao de data
**Tipo**: Dimensao
**Grain**: Uma linha por data unica

| Campo | Tipo | Descricao |
|-------|------|-----------|
| date_key | INT | PK (formato YYYYMMDD) |
| full_date | DATE | Data completa |
| year | INT | Ano |
| quarter | INT | Trimestre (1-4) |
| quarter_name | STRING | Q1, Q2, Q3, Q4 |
| month | INT | Mes (1-12) |
| month_name | STRING | Nome do mes em ingles |
| week_of_year | INT | Semana do ano |
| day_of_month | INT | Dia do mes |
| day_of_week | INT | Dia da semana (1=Domingo) |
| day_name | STRING | Nome do dia em ingles |
| is_weekend | BOOLEAN | True se sabado ou domingo |
| year_month | STRING | Formato YYYY-MM |

---

### 6.2 dim_region

**Proposito**: Dimensao de regiao geografica
**Tipo**: Dimensao
**Grain**: Uma linha por regiao unica

| Campo | Tipo | Descricao |
|-------|------|-----------|
| region_key | INT | PK (surrogate key) |
| region_name | STRING | Nome original (CANADA, MIDWEST, etc) |
| region_group | STRING | Agrupamento (US_EAST, US_WEST, CANADA) |
| country | STRING | Pais (USA ou CANADA) |

**Dados**:

| region_key | region_name | region_group | country |
|------------|-------------|--------------|---------|
| 1 | NORTHEAST | US_EAST | USA |
| 2 | SOUTHEAST | US_EAST | USA |
| 3 | GREAT LAKES | US_EAST | USA |
| 4 | MIDWEST | US_WEST | USA |
| 5 | SOUTHWEST | US_WEST | USA |
| 6 | WEST | US_WEST | USA |
| 7 | CANADA | CANADA | CANADA |

---

### 6.3 dim_product

**Proposito**: Dimensao de produto/marca
**Tipo**: Dimensao
**Grain**: Uma linha por combinacao unica de produto

| Campo | Tipo | Descricao |
|-------|------|-----------|
| product_key | INT | PK (surrogate key) |
| ce_brand_flvr | STRING | Codigo do produto/sabor |
| brand_nm | STRING | Nome da marca |
| pkg_cat | STRING | Codigo categoria embalagem |
| pkg_cat_desc | STRING | Descricao categoria embalagem |
| tsr_pckg_nm | STRING | Nome do pacote |

**Marcas disponiveis**:
- GRAPE
- LEMON
- RASPBERRY
- STRAWBERRY

---

### 6.4 dim_channel

**Proposito**: Dimensao de canal de vendas
**Tipo**: Dimensao
**Grain**: Uma linha por canal unico

| Campo | Tipo | Descricao |
|-------|------|-----------|
| channel_key | INT | PK (surrogate key) |
| chnl_group | STRING | Grupo de canal |
| trade_chnl_desc | STRING | Tipo de canal (natural key) |
| trade_group_desc | STRING | Grupo de trade |
| trade_type_desc | STRING | Tipo (ALCOHOLIC, NON ALCOHOLIC, MIX) |
| is_alcoholic_channel | BOOLEAN | Indica se vende alcool |

---

### 6.5 fact_sales

**Proposito**: Tabela fato de vendas
**Tipo**: Fato
**Grain**: Uma linha por combinacao unica de date/region/product/channel
**Particionamento**: year, month

| Campo | Tipo | Descricao |
|-------|------|-----------|
| date_key | INT | FK -> dim_date |
| region_key | INT | FK -> dim_region |
| product_key | INT | FK -> dim_product |
| channel_key | INT | FK -> dim_channel |
| dollar_volume | DECIMAL(18,2) | Medida: Volume em dolares |
| volume_category | STRING | HIGH, MEDIUM, LOW |
| is_negative_sale | BOOLEAN | Indica devolucao/estorno |
| year | INT | Ano (para particionamento) |
| month | INT | Mes (para particionamento) |
| _consumption_timestamp | TIMESTAMP | Timestamp do processamento |

---

### 6.6 agg_sales_region_tradegroup

**Proposito**: Responde pergunta 4.1 - Top 3 Trade Groups por Regiao
**Tipo**: Tabela agregada

| Campo | Tipo | Descricao |
|-------|------|-----------|
| region_name | STRING | Regiao |
| trade_group_desc | STRING | Grupo de trade |
| total_dollar_volume | DECIMAL(18,2) | Soma de vendas |
| transaction_count | BIGINT | Quantidade de transacoes |
| rank | INT | Ranking (1 = maior volume) |
| _agg_timestamp | TIMESTAMP | Timestamp da agregacao |

---

### 6.7 agg_sales_brand_month

**Proposito**: Responde pergunta 4.2 - Vendas por Marca por Mes
**Tipo**: Tabela agregada

| Campo | Tipo | Descricao |
|-------|------|-----------|
| brand_nm | STRING | Nome da marca |
| year | INT | Ano |
| month | INT | Mes |
| year_month | STRING | Ano-Mes (YYYY-MM) |
| total_dollar_volume | DECIMAL(18,2) | Soma de vendas |
| transaction_count | BIGINT | Quantidade de transacoes |
| _agg_timestamp | TIMESTAMP | Timestamp da agregacao |

---

### 6.8 agg_sales_brand_region

**Proposito**: Responde pergunta 4.3 - Menor Marca por Regiao
**Tipo**: Tabela agregada

| Campo | Tipo | Descricao |
|-------|------|-----------|
| region_name | STRING | Regiao |
| brand_nm | STRING | Nome da marca |
| total_dollar_volume | DECIMAL(18,2) | Soma de vendas |
| transaction_count | BIGINT | Quantidade de transacoes |
| rank_asc | INT | Ranking ascendente (1 = menor venda) |
| rank_desc | INT | Ranking descendente (1 = maior venda) |
| _agg_timestamp | TIMESTAMP | Timestamp da agregacao |

---

## 7. Control Layer

### 7.1 process_control

**Proposito**: Rastreabilidade de todos os processos
**Formato**: Delta Lake

| Campo | Tipo | Descricao |
|-------|------|-----------|
| process_id | STRING | UUID unico da execucao |
| batch_id | STRING | ID do lote |
| layer | STRING | Camada: bronze, silver, gold, consumption |
| table_name | STRING | Nome da tabela processada |
| status | STRING | RUNNING, SUCCESS, FAILED, PARTIAL |
| start_timestamp | TIMESTAMP | Inicio do processamento |
| end_timestamp | TIMESTAMP | Fim do processamento |
| duration_seconds | DECIMAL(10,2) | Tempo de execucao |
| records_read | BIGINT | Registros lidos |
| records_written | BIGINT | Registros escritos |
| records_quarantined | BIGINT | Registros em quarentena |
| records_failed | BIGINT | Registros com erro |
| error_message | STRING | Mensagem de erro |
| error_stack_trace | STRING | Stack trace completo |
| spark_app_id | STRING | ID da aplicacao Spark |
| cluster_id | STRING | ID do cluster |
| created_at | TIMESTAMP | Data de criacao |

---

### 7.2 quarantine

**Proposito**: Armazenar registros com erro para analise
**Formato**: Delta Lake

| Campo | Tipo | Descricao |
|-------|------|-----------|
| quarantine_id | STRING | UUID unico |
| batch_id | STRING | ID do lote de origem |
| source_table | STRING | Tabela de origem |
| target_table | STRING | Tabela de destino |
| record_data | STRING | Linha completa em JSON |
| error_type | STRING | VALIDATION_ERROR, UNKNOWN_ERROR |
| error_code | STRING | Codigo do erro (ex: DQ_SALES_001) |
| error_description | STRING | Descricao do erro |
| dq_rule_name | STRING | Nome da regra que falhou |
| is_known_rule | BOOLEAN | True se erro conhecido |
| reprocessed | BOOLEAN | True se ja reprocessado |
| reprocess_batch_id | STRING | Batch do reprocessamento |
| created_at | TIMESTAMP | Data da quarentena |
| updated_at | TIMESTAMP | Ultima atualizacao |

---

## 8. Regras de Data Quality

### 8.1 beverage_sales

| Codigo | Regra | Campo | Descricao | Acao |
|--------|-------|-------|-----------|------|
| DQ_SALES_001 | NOT_NULL | brand_nm | Marca nao pode ser nula | Quarentena |
| DQ_SALES_002 | NOT_NULL | trade_chnl_desc | Canal nao pode ser nulo | Quarentena |
| DQ_SALES_003 | IS_NUMERIC | dollar_volume | Volume deve ser numerico | Quarentena |
| DQ_SALES_004 | DATE_FORMAT | date | Data deve ser M/D/YYYY | Quarentena |
| DQ_SALES_005 | TRIM_WHITESPACE | brand_nm | Remover espacos | Corrigir |
| DQ_SALES_006 | VALID_YEAR | year | Ano entre 2000-2030 | Quarentena |
| DQ_SALES_007 | VALID_MONTH | month | Mes entre 1-12 | Quarentena |
| DQ_SALES_008 | REFERENTIAL | trade_chnl_desc | Deve existir em channels | Quarentena |

### 8.2 channel_features

| Codigo | Regra | Campo | Descricao | Acao |
|--------|-------|-------|-----------|------|
| DQ_CHAN_001 | NOT_NULL | trade_chnl_desc | Canal nao pode ser nulo | Quarentena |
| DQ_CHAN_002 | NOT_NULL | trade_group_desc | Grupo nao pode ser nulo | Quarentena |
| DQ_CHAN_003 | UNIQUE | trade_chnl_desc | Canal deve ser unico | Quarentena |
| DQ_CHAN_004 | VALID_VALUES | trade_type_desc | Valores permitidos | Quarentena |

---

## 9. Regras de Negocio (Gold Layer)

### 9.1 volume_category

| Condicao | Valor |
|----------|-------|
| dollar_volume > 1000 | HIGH |
| dollar_volume > 100 | MEDIUM |
| dollar_volume <= 100 | LOW |

### 9.2 region_group

| region | region_group |
|--------|--------------|
| NORTHEAST | US_EAST |
| SOUTHEAST | US_EAST |
| GREAT LAKES | US_EAST |
| MIDWEST | US_WEST |
| SOUTHWEST | US_WEST |
| WEST | US_WEST |
| CANADA | CANADA |

### 9.3 sales_quarter

| month | sales_quarter |
|-------|---------------|
| 1, 2, 3 | Q1 |
| 4, 5, 6 | Q2 |
| 7, 8, 9 | Q3 |
| 10, 11, 12 | Q4 |

---

## 10. Linhagem de Dados

```
LANDING (CSV)
    |
    v
BRONZE (Delta)
    - Padronizacao de nomes
    - Campos de auditoria
    |
    v
SILVER (Delta)
    - Data Quality
    - Tipagem
    - JOIN sales + channels
    |
    v
GOLD (Delta)
    - Regras de negocio
    - Campos calculados
    - Categorizacoes
    |
    v
CONSUMPTION (Delta)
    - Dimensoes
    - Fato
    - Agregacoes
    |
    v
BI / DASHBOARDS
```

---

Ultima atualizacao: 01/12/2024
