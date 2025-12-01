# ==============================================================================
# Data Factory Module - Variables
# ==============================================================================

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "data_factory_name" {
  type = string
}

variable "hdinsight_cluster_id" {
  type = string
}

variable "storage_account_name" {
  type = string
}

variable "storage_account_key" {
  type      = string
  sensitive = true
}

variable "pipeline_schedule" {
  type    = string
  default = "02:00"
}

variable "tags" {
  type    = map(string)
  default = {}
}

