# ==============================================================================
# HDInsight Module - Variables
# ==============================================================================

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "cluster_name" {
  type = string
}

variable "cluster_version" {
  type    = string
  default = "5.1"
}

variable "spark_version" {
  type    = string
  default = "3.3"
}

variable "head_node_vm_size" {
  type    = string
  default = "Standard_D4_v2"
}

variable "worker_node_vm_size" {
  type    = string
  default = "Standard_D4_v2"
}

variable "worker_node_count_min" {
  type    = number
  default = 2
}

variable "worker_node_count_max" {
  type    = number
  default = 10
}

variable "username" {
  type = string
}

variable "password" {
  type      = string
  sensitive = true
}

variable "ssh_username" {
  type = string
}

variable "ssh_password" {
  type      = string
  sensitive = true
}

variable "storage_account_name" {
  type = string
}

variable "storage_account_key" {
  type      = string
  sensitive = true
}

variable "tags" {
  type    = map(string)
  default = {}
}

