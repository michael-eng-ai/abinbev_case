# ==============================================================================
# HDInsight Module - Apache Spark Cluster
# ==============================================================================

resource "azurerm_hdinsight_spark_cluster" "spark" {
  name                = var.cluster_name
  resource_group_name = var.resource_group_name
  location            = var.location
  cluster_version     = var.cluster_version
  tier                = "Standard"

  component_version {
    spark = var.spark_version
  }

  gateway {
    username = var.username
    password = var.password
  }

  storage_account {
    storage_container_id = "https://${var.storage_account_name}.blob.core.windows.net/scripts"
    storage_account_key  = var.storage_account_key
    is_default           = true
  }

  roles {
    head_node {
      vm_size  = var.head_node_vm_size
      username = var.ssh_username
      password = var.ssh_password
    }

    worker_node {
      vm_size               = var.worker_node_vm_size
      username              = var.ssh_username
      password              = var.ssh_password
      target_instance_count = var.worker_node_count_min

      autoscale {
        capacity {
          min_instance_count = var.worker_node_count_min
          max_instance_count = var.worker_node_count_max
        }
      }
    }

    zookeeper_node {
      vm_size  = "Standard_A4_v2"
      username = var.ssh_username
      password = var.ssh_password
    }
  }

  tags = var.tags
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "cluster_name" {
  value = azurerm_hdinsight_spark_cluster.spark.name
}

output "cluster_id" {
  value = azurerm_hdinsight_spark_cluster.spark.id
}

output "https_endpoint" {
  value = azurerm_hdinsight_spark_cluster.spark.https_endpoint
}

output "ssh_endpoint" {
  value = azurerm_hdinsight_spark_cluster.spark.ssh_endpoint
}

