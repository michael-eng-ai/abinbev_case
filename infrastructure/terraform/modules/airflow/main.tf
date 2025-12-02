# ==============================================================================
# Airflow Module - Apache Airflow on Azure Container Instances
# ==============================================================================
#
# Deploy Apache Airflow como orquestrador de pipelines.
# Usa Azure Container Instances para simplicidade.
# Para producao, considerar AKS com Helm chart oficial.
#

# Container Group para Airflow
resource "azurerm_container_group" "airflow" {
  name                = "${var.prefix}-airflow"
  location            = var.location
  resource_group_name = var.resource_group_name
  os_type             = "Linux"
  ip_address_type     = "Public"
  dns_name_label      = "${var.prefix}-airflow"

  # Airflow Webserver
  container {
    name   = "airflow-webserver"
    image  = "apache/airflow:2.8.0"
    cpu    = "1"
    memory = "2"

    ports {
      port     = 8080
      protocol = "TCP"
    }

    environment_variables = {
      AIRFLOW__CORE__EXECUTOR                  = "LocalExecutor"
      AIRFLOW__CORE__SQL_ALCHEMY_CONN          = "postgresql+psycopg2://${var.db_user}:${var.db_password}@${var.db_host}:5432/${var.db_name}"
      AIRFLOW__CORE__FERNET_KEY                = var.fernet_key
      AIRFLOW__WEBSERVER__SECRET_KEY           = var.secret_key
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION = "False"
      AIRFLOW__CORE__LOAD_EXAMPLES             = "False"
      AZURE_STORAGE_ACCOUNT_NAME               = var.storage_account_name
      AZURE_STORAGE_ACCOUNT_KEY                = var.storage_account_key
    }

    commands = ["airflow", "webserver"]

    volume {
      name       = "dags"
      mount_path = "/opt/airflow/dags"
      read_only  = false
      share_name = "airflow-dags"
      storage_account_name = var.storage_account_name
      storage_account_key  = var.storage_account_key
    }
  }

  # Airflow Scheduler
  container {
    name   = "airflow-scheduler"
    image  = "apache/airflow:2.8.0"
    cpu    = "1"
    memory = "2"

    environment_variables = {
      AIRFLOW__CORE__EXECUTOR                  = "LocalExecutor"
      AIRFLOW__CORE__SQL_ALCHEMY_CONN          = "postgresql+psycopg2://${var.db_user}:${var.db_password}@${var.db_host}:5432/${var.db_name}"
      AIRFLOW__CORE__FERNET_KEY                = var.fernet_key
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION = "False"
      AIRFLOW__CORE__LOAD_EXAMPLES             = "False"
      AZURE_STORAGE_ACCOUNT_NAME               = var.storage_account_name
      AZURE_STORAGE_ACCOUNT_KEY                = var.storage_account_key
    }

    commands = ["airflow", "scheduler"]

    volume {
      name       = "dags"
      mount_path = "/opt/airflow/dags"
      read_only  = false
      share_name = "airflow-dags"
      storage_account_name = var.storage_account_name
      storage_account_key  = var.storage_account_key
    }
  }

  tags = var.tags
}

# PostgreSQL para Airflow Metadata
resource "azurerm_postgresql_flexible_server" "airflow_db" {
  count               = var.create_database ? 1 : 0
  name                = "${var.prefix}-airflow-db"
  resource_group_name = var.resource_group_name
  location            = var.location
  version             = "14"
  
  administrator_login    = var.db_user
  administrator_password = var.db_password
  
  storage_mb = 32768
  sku_name   = "B_Standard_B1ms"

  tags = var.tags
}

resource "azurerm_postgresql_flexible_server_database" "airflow" {
  count     = var.create_database ? 1 : 0
  name      = var.db_name
  server_id = azurerm_postgresql_flexible_server.airflow_db[0].id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

# Azure File Share para DAGs
resource "azurerm_storage_share" "dags" {
  name                 = "airflow-dags"
  storage_account_id   = var.storage_account_id
  quota                = 5
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "airflow_url" {
  value = "http://${azurerm_container_group.airflow.fqdn}:8080"
}

output "airflow_fqdn" {
  value = azurerm_container_group.airflow.fqdn
}

output "dags_share_name" {
  value = azurerm_storage_share.dags.name
}

