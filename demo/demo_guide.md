# Guia de Demonstração (Step-by-Step)

Este guia foi preparado para auxiliar na demonstração do projeto técnico para o Case ABInBev.

## 1. Preparação do Ambiente
Abra **3 abas** no terminal na pasta raiz do projeto (`cd abinbev_case`).

**Terminal 1: Airflow Webserver**
```bash
# Inicia a interface web do Airflow
export AIRFLOW_HOME="$(pwd)/airflow_local"
poetry run airflow webserver -p 8080
```
*Acesse em: [http://localhost:8080](http://localhost:8080)* (Login: `admin` / `admin`)

**Terminal 2: Airflow Scheduler**
```bash
# Inicia o agendador de tarefas
export AIRFLOW_HOME="$(pwd)/airflow_local"
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
poetry run airflow scheduler
```

**Terminal 3: Jupyter Notebook**
```bash
# Inicia o ambiente de consultas interativas
poetry run jupyter notebook notebooks/interactive_queries.ipynb
```

---

## 2. Execução do Pipeline (Airflow)
Demonstre a orquestração e execução automática dos dados.

1.  Acesse o Airflow: [http://localhost:8080](http://localhost:8080).
2.  Localize a DAG: **`abinbev_case_medallion`**.
3.  **Ative a DAG** (toggle switch "ON" à esquerda).
4.  Clique no botão **Play** (triângulo) do lado direito e selecione "Trigger DAG".
5.  Vá para a aba **"Grid"** (ou "Graph") e acompanhe a execução:
    *   Verde Escuro = Sucesso.
    *   Observe as etapas: `check_source_files` -> `bronze` -> `silver` -> `gold` -> `consumption`.

**Destaque:** Explique que o pipeline segue a arquitetura Medallion e é idempotente.

---

## 3. Análise de Dados e Negócio (Jupyter)
Demonstre os resultados finais e a capacidade de responder perguntas de negócio.

1.  Volte para a aba do **Jupyter Notebook**.
2.  Abra o arquivo `interactive_queries.ipynb`.
3.  Execute a célula de **Load Tables** (Seção 1) para carregar os dados atualizados.
4.  **Seção 2: Business Questions**:
    *   **2.1 Top 3 Trade Groups by Region**: Mostre o resultado da query.
    *   **2.2 Sales by Brand per Month**: Mostre a agregação temporal.
5.  **Destaque:** Explique que os dados estão sendo lidos da camada `consumption` (Star Schema), otimizada para Analytics e BI.

---

## 4. Observabilidade e Qualidade (Jupyter)
Esta é a parte que diferencia sua entrega, mostrando robustez.

1.  Vá para a **Seção 3: Observability & Control** no notebook.
2.  Execute a query **3.1 Recent Pipeline Executions**:
    *   Mostre a tabela `process_control`.
    *   Explique as colunas: `status`, `duration`, `records_read` vs `records_written`.
    *   Isso prova que você monitora a performance e o sucesso de cada etapa.
3.  Execute a query **3.2 Quarantine Analysis**:
    *   Mostre a tabela `quarantine` (pode estar vazia se os dados forem perfeitos).
    *   Explique que **dados inválidos não quebram o pipeline**, eles são desviados para quarentena para análise posterior.
4.  Execute a célula **3.3 Visual Observability**:
    *   Mostre o gráfico de barras gerado.
    *   **Destaque:** "Temos visão visual imediata do volume de dados processados vs quarentenados."

---

## 5. Showcase de Engenharia Avançada (Governança)
Mostre que o projeto está pronto para ambientes enterprise, mesmo rodando localmente.

1.  Ainda no notebook, vá para a **Seção 4: Governance & Architecture**.
2.  **4.1 Access Policies**: Execute a célula para mostrar o arquivo `governance_policies.yaml`.
    *   **Discurso**: "Aqui definimos as políticas de acesso e segurança (PII) que são aplicadas pelo OpenMetadata no ambiente de produção."
3.  **4.2 Observability Rules**: Mostre o arquivo `alert_rules.yml`.
    *   **Discurso**: "Aqui estão as regras de alerta do Prometheus configuradas para disparar notificações no Slack caso a taxa de erro suba."
4.  **4.3 Data Catalog Integration**: Mostre o código de ingestão.
    *   **Discurso**: "O pipeline é autoconsciente: este código registra metadados e linhagem automaticamente no catálogo a cada execução."

---

## Extras (Se perguntarem)
*   **Infraestrutura**: Mencione o Terraform (Azure HDInsight, Data Lake Gen2) e que o ambiente local simula essa arquitetura usando Spark Local e sistema de arquivos.
*   **Testes**: Mencione que existem testes unitários (`pytest`) garantindo a qualidade das transformações.
