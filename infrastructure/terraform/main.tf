# ==============================================================================
# ABInBev Case - Terraform Main Configuration
# ==============================================================================
# 
# Este arquivo provisiona a infraestrutura completa na Azure:
# - Resource Group
# - Storage Account (Data Lake Gen2)
# - HDInsight Spark Cluster com Autoscaling
# - Apache Airflow (orquestracao)
# - Stack de Observabilidade (Prometheus, Grafana, Loki)
# - Governanca de Dados (OpenMetadata)
#
# ==============================================================================

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }

  # Backend para state remoto (descomentar para producao)
  # backend "azurerm" {
  #   resource_group_name  = "terraform-state-rg"
  #   storage_account_name = "tfstateabinbev"
  #   container_name       = "tfstate"
  #   key                  = "abinbev-case.tfstate"
  # }
}

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

# ------------------------------------------------------------------------------
# Locals
# ------------------------------------------------------------------------------

locals {
  resource_prefix = "${var.project_name}-${var.environment}"
  
  common_tags = merge(var.tags, {
    Environment = var.environment
    Terraform   = "true"
  })
}

# ------------------------------------------------------------------------------
# Random suffix para nomes unicos
# ------------------------------------------------------------------------------

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

# ------------------------------------------------------------------------------
# Resource Group
# ------------------------------------------------------------------------------

resource "azurerm_resource_group" "main" {
  name     = "${local.resource_prefix}-rg"
  location = var.location
  tags     = local.common_tags
}

# ------------------------------------------------------------------------------
# Storage Module
# ------------------------------------------------------------------------------

module "storage" {
  source = "./modules/storage"

  resource_group_name  = azurerm_resource_group.main.name
  location             = azurerm_resource_group.main.location
  storage_account_name = "${var.storage_account_name}${random_string.suffix.result}"
  containers           = var.storage_containers
  replication_type     = var.storage_replication_type
  tags                 = local.common_tags
}

# ------------------------------------------------------------------------------
# HDInsight Module
# ------------------------------------------------------------------------------

module "hdinsight" {
  source = "./modules/hdinsight"

  resource_group_name   = azurerm_resource_group.main.name
  location              = azurerm_resource_group.main.location
  cluster_name          = "${var.hdinsight_cluster_name}-${random_string.suffix.result}"
  cluster_version       = var.hdinsight_cluster_version
  spark_version         = var.hdinsight_spark_version
  head_node_vm_size     = var.hdinsight_head_node_vm_size
  worker_node_vm_size   = var.hdinsight_worker_node_vm_size
  worker_node_count_min = var.hdinsight_worker_node_count_min
  worker_node_count_max = var.hdinsight_worker_node_count_max
  username              = var.hdinsight_username
  password              = var.hdinsight_password
  ssh_username          = var.hdinsight_ssh_username
  ssh_password          = var.hdinsight_ssh_password
  storage_account_name  = module.storage.storage_account_name
  storage_account_key   = module.storage.storage_account_key
  tags                  = local.common_tags
}

# ------------------------------------------------------------------------------
# Airflow Module (Orquestracao)
# ------------------------------------------------------------------------------

module "airflow" {
  source = "./modules/airflow"
  count  = var.airflow_enabled ? 1 : 0

  prefix               = local.resource_prefix
  resource_group_name  = azurerm_resource_group.main.name
  location             = azurerm_resource_group.main.location
  storage_account_name = module.storage.storage_account_name
  storage_account_key  = module.storage.storage_account_key
  storage_account_id   = module.storage.storage_account_id
  db_password          = var.airflow_db_password
  fernet_key           = var.airflow_fernet_key
  secret_key           = var.airflow_secret_key
  tags                 = local.common_tags
}

# ------------------------------------------------------------------------------
# Observability Module (Prometheus, Grafana, Loki)
# ------------------------------------------------------------------------------

module "observability" {
  source = "./modules/observability"
  count  = var.observability_enabled ? 1 : 0

  resource_group_name    = azurerm_resource_group.main.name
  location               = azurerm_resource_group.main.location
  resource_prefix        = local.resource_prefix
  grafana_admin_password = var.grafana_admin_password
  tags                   = local.common_tags
}

# ------------------------------------------------------------------------------
# Governance Module (OpenMetadata)
# ------------------------------------------------------------------------------

module "governance" {
  source = "./modules/governance"
  count  = var.governance_enabled ? 1 : 0

  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  resource_prefix     = local.resource_prefix
  db_admin_username   = var.openmetadata_db_username
  db_admin_password   = var.openmetadata_db_password
  tags                = local.common_tags
}

