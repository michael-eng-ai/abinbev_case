# ==============================================================================
# Storage Module - Azure Data Lake Storage Gen2
# ==============================================================================

resource "azurerm_storage_account" "datalake" {
  name                     = var.storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = var.replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Hierarchical namespace para Data Lake Gen2
  
  tags = var.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "containers" {
  for_each           = toset(var.containers)
  name               = each.value
  storage_account_id = azurerm_storage_account.datalake.id
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "storage_account_id" {
  value = azurerm_storage_account.datalake.id
}

output "storage_account_key" {
  value     = azurerm_storage_account.datalake.primary_access_key
  sensitive = true
}

output "primary_dfs_endpoint" {
  value = azurerm_storage_account.datalake.primary_dfs_endpoint
}

output "container_names" {
  value = [for c in azurerm_storage_data_lake_gen2_filesystem.containers : c.name]
}

