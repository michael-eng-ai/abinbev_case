# ==============================================================================
# Data Factory Module - Azure Data Factory
# ==============================================================================

resource "azurerm_data_factory" "adf" {
  name                = var.data_factory_name
  location            = var.location
  resource_group_name = var.resource_group_name
  
  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# Linked Service para Storage Account
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "storage" {
  name                 = "ls_datalake"
  data_factory_id      = azurerm_data_factory.adf.id
  url                  = "https://${var.storage_account_name}.dfs.core.windows.net"
  storage_account_key  = var.storage_account_key
}

# Linked Service para HDInsight
resource "azurerm_data_factory_linked_service_azure_blob_storage" "hdinsight_storage" {
  name              = "ls_hdinsight_storage"
  data_factory_id   = azurerm_data_factory.adf.id
  connection_string = "DefaultEndpointsProtocol=https;AccountName=${var.storage_account_name};AccountKey=${var.storage_account_key}"
}

# Trigger para execucao diaria
resource "azurerm_data_factory_trigger_schedule" "daily" {
  name            = "trigger_daily_pipeline"
  data_factory_id = azurerm_data_factory.adf.id
  
  interval  = 1
  frequency = "Day"
  
  start_time = "2024-12-01T${var.pipeline_schedule}:00Z"
  time_zone  = "UTC"
  
  activated = true
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "data_factory_name" {
  value = azurerm_data_factory.adf.name
}

output "data_factory_id" {
  value = azurerm_data_factory.adf.id
}

output "data_factory_identity" {
  value = azurerm_data_factory.adf.identity[0].principal_id
}

