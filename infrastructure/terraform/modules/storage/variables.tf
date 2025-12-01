# ==============================================================================
# Storage Module - Variables
# ==============================================================================

variable "resource_group_name" {
  description = "Nome do Resource Group"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "storage_account_name" {
  description = "Nome do Storage Account"
  type        = string
}

variable "containers" {
  description = "Lista de containers"
  type        = list(string)
}

variable "replication_type" {
  description = "Tipo de replicacao"
  type        = string
  default     = "LRS"
}

variable "tags" {
  description = "Tags"
  type        = map(string)
  default     = {}
}

