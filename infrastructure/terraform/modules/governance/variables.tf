# ==============================================================================
# Governance Module - Variables
# ==============================================================================

variable "resource_group_name" {
  description = "Nome do Resource Group"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "resource_prefix" {
  description = "Prefixo para nomes dos recursos"
  type        = string
}

variable "db_admin_username" {
  description = "Username do administrador do PostgreSQL"
  type        = string
  default     = "openmetadata_admin"
}

variable "db_admin_password" {
  description = "Password do administrador do PostgreSQL"
  type        = string
  sensitive   = true
}

variable "tags" {
  description = "Tags para os recursos"
  type        = map(string)
  default     = {}
}

