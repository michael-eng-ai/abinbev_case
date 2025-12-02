# ==============================================================================
# Airflow Module - Variables
# ==============================================================================

variable "prefix" {
  description = "Prefixo para nomes dos recursos"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "resource_group_name" {
  description = "Nome do Resource Group"
  type        = string
}

variable "storage_account_name" {
  description = "Nome da Storage Account"
  type        = string
}

variable "storage_account_key" {
  description = "Chave da Storage Account"
  type        = string
  sensitive   = true
}

variable "storage_account_id" {
  description = "ID da Storage Account"
  type        = string
}

variable "create_database" {
  description = "Criar PostgreSQL para Airflow"
  type        = bool
  default     = true
}

variable "db_host" {
  description = "Host do PostgreSQL"
  type        = string
  default     = ""
}

variable "db_name" {
  description = "Nome do banco de dados"
  type        = string
  default     = "airflow"
}

variable "db_user" {
  description = "Usuario do banco"
  type        = string
  default     = "airflow_admin"
}

variable "db_password" {
  description = "Senha do banco"
  type        = string
  sensitive   = true
}

variable "fernet_key" {
  description = "Fernet key para criptografia do Airflow"
  type        = string
  sensitive   = true
}

variable "secret_key" {
  description = "Secret key para o webserver"
  type        = string
  sensitive   = true
}

variable "tags" {
  description = "Tags para os recursos"
  type        = map(string)
  default     = {}
}

