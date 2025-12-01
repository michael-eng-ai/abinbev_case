# ==============================================================================
# ABInBev Case - Terraform Outputs
# ==============================================================================

# ------------------------------------------------------------------------------
# Resource Group
# ------------------------------------------------------------------------------

output "resource_group_name" {
  description = "Nome do Resource Group"
  value       = azurerm_resource_group.main.name
}

output "resource_group_location" {
  description = "Localizacao do Resource Group"
  value       = azurerm_resource_group.main.location
}

# ------------------------------------------------------------------------------
# Storage
# ------------------------------------------------------------------------------

output "storage_account_name" {
  description = "Nome do Storage Account"
  value       = module.storage.storage_account_name
}

output "storage_account_primary_endpoint" {
  description = "Endpoint primario do Storage Account"
  value       = module.storage.primary_dfs_endpoint
}

output "storage_containers" {
  description = "Lista de containers criados"
  value       = module.storage.container_names
}

# ------------------------------------------------------------------------------
# HDInsight
# ------------------------------------------------------------------------------

output "hdinsight_cluster_name" {
  description = "Nome do cluster HDInsight"
  value       = module.hdinsight.cluster_name
}

output "hdinsight_cluster_id" {
  description = "ID do cluster HDInsight"
  value       = module.hdinsight.cluster_id
}

output "hdinsight_https_endpoint" {
  description = "Endpoint HTTPS do cluster"
  value       = module.hdinsight.https_endpoint
}

output "hdinsight_ssh_endpoint" {
  description = "Endpoint SSH do cluster"
  value       = module.hdinsight.ssh_endpoint
}

# ------------------------------------------------------------------------------
# Data Factory
# ------------------------------------------------------------------------------

output "data_factory_name" {
  description = "Nome do Azure Data Factory"
  value       = module.data_factory.data_factory_name
}

output "data_factory_id" {
  description = "ID do Azure Data Factory"
  value       = module.data_factory.data_factory_id
}

# ------------------------------------------------------------------------------
# Observability
# ------------------------------------------------------------------------------

output "grafana_url" {
  description = "URL do Grafana"
  value       = var.observability_enabled ? module.observability[0].grafana_url : "Observability disabled"
}

output "prometheus_url" {
  description = "URL do Prometheus"
  value       = var.observability_enabled ? module.observability[0].prometheus_url : "Observability disabled"
}

# ------------------------------------------------------------------------------
# Connection Strings (para notebooks)
# ------------------------------------------------------------------------------

output "storage_connection_string" {
  description = "Connection string para uso nos notebooks"
  value       = "abfss://{container}@${module.storage.storage_account_name}.dfs.core.windows.net/"
  sensitive   = false
}

output "environment_variables" {
  description = "Variaveis de ambiente para configurar nos notebooks"
  value = {
    STORAGE_ACCOUNT_NAME = module.storage.storage_account_name
    STORAGE_CONTAINER_LANDING = "landing"
    STORAGE_CONTAINER_BRONZE = "bronze"
    STORAGE_CONTAINER_SILVER = "silver"
    STORAGE_CONTAINER_GOLD = "gold"
    STORAGE_CONTAINER_CONSUMPTION = "consumption"
    STORAGE_CONTAINER_CONTROL = "control"
    HDINSIGHT_CLUSTER_NAME = module.hdinsight.cluster_name
    DATA_FACTORY_NAME = module.data_factory.data_factory_name
    ENVIRONMENT = var.environment
  }
}

# ------------------------------------------------------------------------------
# Governance (OpenMetadata)
# ------------------------------------------------------------------------------

output "openmetadata_url" {
  description = "URL do OpenMetadata"
  value       = var.governance_enabled ? module.governance[0].openmetadata_url : "Governance disabled"
}

output "openmetadata_fqdn" {
  description = "FQDN do OpenMetadata"
  value       = var.governance_enabled ? module.governance[0].openmetadata_fqdn : "Governance disabled"
}

