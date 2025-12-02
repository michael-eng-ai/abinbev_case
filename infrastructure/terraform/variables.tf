# ==============================================================================
# ABInBev Case - Terraform Variables
# ==============================================================================

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------

variable "project_name" {
  description = "Nome do projeto"
  type        = string
  default     = "abinbev-case"
}

variable "environment" {
  description = "Ambiente (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "East US 2"
}

variable "tags" {
  description = "Tags para recursos"
  type        = map(string)
  default = {
    Project     = "ABInBev Case"
    Environment = "dev"
    ManagedBy   = "Terraform"
  }
}

# ------------------------------------------------------------------------------
# Storage
# ------------------------------------------------------------------------------

variable "storage_account_name" {
  description = "Nome do Storage Account (deve ser unico globalmente)"
  type        = string
  default     = "abinbevdatalake"
}

variable "storage_containers" {
  description = "Lista de containers do Data Lake"
  type        = list(string)
  default     = ["landing", "bronze", "silver", "gold", "consumption", "control", "scripts"]
}

variable "storage_replication_type" {
  description = "Tipo de replicacao (LRS, GRS, ZRS)"
  type        = string
  default     = "LRS"
}

# ------------------------------------------------------------------------------
# HDInsight
# ------------------------------------------------------------------------------

variable "hdinsight_cluster_name" {
  description = "Nome do cluster HDInsight"
  type        = string
  default     = "abinbev-spark-cluster"
}

variable "hdinsight_cluster_version" {
  description = "Versao do cluster HDInsight"
  type        = string
  default     = "5.1"
}

variable "hdinsight_spark_version" {
  description = "Versao do Spark"
  type        = string
  default     = "3.3"
}

variable "hdinsight_head_node_vm_size" {
  description = "Tamanho da VM dos head nodes"
  type        = string
  default     = "Standard_D4_v2"
}

variable "hdinsight_worker_node_vm_size" {
  description = "Tamanho da VM dos worker nodes"
  type        = string
  default     = "Standard_D4_v2"
}

variable "hdinsight_worker_node_count_min" {
  description = "Numero minimo de worker nodes"
  type        = number
  default     = 2
}

variable "hdinsight_worker_node_count_max" {
  description = "Numero maximo de worker nodes (autoscaling)"
  type        = number
  default     = 10
}

variable "hdinsight_username" {
  description = "Username do cluster"
  type        = string
  default     = "sparkadmin"
}

variable "hdinsight_password" {
  description = "Password do cluster (minimo 10 caracteres, letras, numeros e especiais)"
  type        = string
  sensitive   = true
}

variable "hdinsight_ssh_username" {
  description = "Username SSH"
  type        = string
  default     = "sshuser"
}

variable "hdinsight_ssh_password" {
  description = "Password SSH"
  type        = string
  sensitive   = true
}

# ------------------------------------------------------------------------------
# Airflow
# ------------------------------------------------------------------------------

variable "airflow_enabled" {
  description = "Habilitar Apache Airflow para orquestracao"
  type        = bool
  default     = true
}

variable "airflow_db_password" {
  description = "Password do PostgreSQL para Airflow"
  type        = string
  sensitive   = true
  default     = "Airflow@123456"
}

variable "airflow_fernet_key" {
  description = "Fernet key para criptografia do Airflow (gerar com: python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
  type        = string
  sensitive   = true
  default     = ""
}

variable "airflow_secret_key" {
  description = "Secret key para o webserver do Airflow"
  type        = string
  sensitive   = true
  default     = ""
}

# ------------------------------------------------------------------------------
# Observability
# ------------------------------------------------------------------------------

variable "observability_enabled" {
  description = "Habilitar stack de observabilidade"
  type        = bool
  default     = true
}

variable "grafana_admin_password" {
  description = "Password do admin do Grafana"
  type        = string
  sensitive   = true
  default     = "Admin@123456"
}

# ------------------------------------------------------------------------------
# Governance (OpenMetadata)
# ------------------------------------------------------------------------------

variable "governance_enabled" {
  description = "Habilitar OpenMetadata para governanca de dados"
  type        = bool
  default     = true
}

variable "openmetadata_db_username" {
  description = "Username do PostgreSQL para OpenMetadata"
  type        = string
  default     = "openmetadata_admin"
}

variable "openmetadata_db_password" {
  description = "Password do PostgreSQL para OpenMetadata"
  type        = string
  sensitive   = true
  default     = "OpenMeta@123456"
}

# ------------------------------------------------------------------------------
# Network (opcional para producao)
# ------------------------------------------------------------------------------

variable "create_vnet" {
  description = "Criar VNet dedicada"
  type        = bool
  default     = false
}

variable "vnet_address_space" {
  description = "Address space da VNet"
  type        = string
  default     = "10.0.0.0/16"
}

