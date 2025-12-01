# ==============================================================================
# Governance Module - OpenMetadata
# ==============================================================================
#
# Deploy do OpenMetadata via Azure Container Instances
# Para producao, considerar Azure Kubernetes Service (AKS)
#
# Componentes:
# - OpenMetadata Server
# - PostgreSQL (metadata store)
# - Elasticsearch (search)
#
# ==============================================================================

# ------------------------------------------------------------------------------
# PostgreSQL para OpenMetadata
# ------------------------------------------------------------------------------

resource "azurerm_postgresql_flexible_server" "openmetadata_db" {
  name                   = "${var.resource_prefix}-openmetadata-db"
  resource_group_name    = var.resource_group_name
  location               = var.location
  version                = "14"
  administrator_login    = var.db_admin_username
  administrator_password = var.db_admin_password
  storage_mb             = 32768
  sku_name               = "B_Standard_B1ms"
  zone                   = "1"

  tags = var.tags
}

resource "azurerm_postgresql_flexible_server_database" "openmetadata" {
  name      = "openmetadata"
  server_id = azurerm_postgresql_flexible_server.openmetadata_db.id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_azure" {
  name             = "AllowAzureServices"
  server_id        = azurerm_postgresql_flexible_server.openmetadata_db.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# ------------------------------------------------------------------------------
# Elasticsearch para OpenMetadata
# ------------------------------------------------------------------------------

resource "azurerm_container_group" "elasticsearch" {
  name                = "${var.resource_prefix}-elasticsearch"
  location            = var.location
  resource_group_name = var.resource_group_name
  os_type             = "Linux"
  ip_address_type     = "Public"
  dns_name_label      = "${var.resource_prefix}-elasticsearch"

  container {
    name   = "elasticsearch"
    image  = "docker.elastic.co/elasticsearch/elasticsearch:7.16.3"
    cpu    = "2"
    memory = "4"

    ports {
      port     = 9200
      protocol = "TCP"
    }

    ports {
      port     = 9300
      protocol = "TCP"
    }

    environment_variables = {
      "discovery.type"         = "single-node"
      "ES_JAVA_OPTS"          = "-Xms512m -Xmx512m"
      "xpack.security.enabled" = "false"
    }
  }

  tags = var.tags
}

# ------------------------------------------------------------------------------
# OpenMetadata Server
# ------------------------------------------------------------------------------

resource "azurerm_container_group" "openmetadata" {
  name                = "${var.resource_prefix}-openmetadata"
  location            = var.location
  resource_group_name = var.resource_group_name
  os_type             = "Linux"
  ip_address_type     = "Public"
  dns_name_label      = "${var.resource_prefix}-openmetadata"

  container {
    name   = "openmetadata"
    image  = "openmetadata/server:1.2.0"
    cpu    = "2"
    memory = "4"

    ports {
      port     = 8585
      protocol = "TCP"
    }

    environment_variables = {
      # Database
      "DB_DRIVER_CLASS"       = "org.postgresql.Driver"
      "DB_SCHEME"             = "postgresql"
      "DB_HOST"               = azurerm_postgresql_flexible_server.openmetadata_db.fqdn
      "DB_PORT"               = "5432"
      "DB_USER"               = var.db_admin_username
      "DB_USER_PASSWORD"      = var.db_admin_password
      "OM_DATABASE"           = "openmetadata"
      
      # Elasticsearch
      "ELASTICSEARCH_HOST"    = azurerm_container_group.elasticsearch.fqdn
      "ELASTICSEARCH_PORT"    = "9200"
      "ELASTICSEARCH_SCHEME"  = "http"
      
      # Authentication
      "AUTHENTICATION_PROVIDER"           = "basic"
      "AUTHENTICATION_PUBLIC_KEYS"        = "[http://localhost:8585/api/v1/system/config/jwks]"
      "AUTHENTICATION_AUTHORITY"          = "http://localhost:8585/api/v1/system/config/jwks"
      "AUTHENTICATION_CALLBACK_URL"       = "http://localhost:8585/callback"
      
      # Airflow (opcional)
      "AIRFLOW_HOST"          = "http://localhost:8080"
      
      # Server
      "SERVER_HOST"           = "0.0.0.0"
      "SERVER_PORT"           = "8585"
      "SERVER_ADMIN_PORT"     = "8586"
      
      # Log
      "LOG_LEVEL"             = "INFO"
    }
  }

  tags = var.tags

  depends_on = [
    azurerm_postgresql_flexible_server_database.openmetadata,
    azurerm_container_group.elasticsearch
  ]
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "openmetadata_url" {
  value = "http://${azurerm_container_group.openmetadata.fqdn}:8585"
}

output "openmetadata_fqdn" {
  value = azurerm_container_group.openmetadata.fqdn
}

output "elasticsearch_url" {
  value = "http://${azurerm_container_group.elasticsearch.fqdn}:9200"
}

output "database_host" {
  value = azurerm_postgresql_flexible_server.openmetadata_db.fqdn
}

