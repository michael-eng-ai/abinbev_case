# ==============================================================================
# Observability Module - Prometheus, Grafana, Loki
# ==============================================================================
#
# Deploy via Azure Container Instances (ACI)
# Para producao, considerar Azure Kubernetes Service (AKS)
#
# ==============================================================================

# ------------------------------------------------------------------------------
# Prometheus
# ------------------------------------------------------------------------------

resource "azurerm_container_group" "prometheus" {
  name                = "${var.resource_prefix}-prometheus"
  location            = var.location
  resource_group_name = var.resource_group_name
  os_type             = "Linux"
  ip_address_type     = "Public"
  dns_name_label      = "${var.resource_prefix}-prometheus"

  container {
    name   = "prometheus"
    image  = "prom/prometheus:latest"
    cpu    = "1"
    memory = "2"

    ports {
      port     = 9090
      protocol = "TCP"
    }
  }

  tags = var.tags
}

# ------------------------------------------------------------------------------
# Loki
# ------------------------------------------------------------------------------

resource "azurerm_container_group" "loki" {
  name                = "${var.resource_prefix}-loki"
  location            = var.location
  resource_group_name = var.resource_group_name
  os_type             = "Linux"
  ip_address_type     = "Public"
  dns_name_label      = "${var.resource_prefix}-loki"

  container {
    name   = "loki"
    image  = "grafana/loki:latest"
    cpu    = "1"
    memory = "2"

    ports {
      port     = 3100
      protocol = "TCP"
    }
  }

  tags = var.tags
}

# ------------------------------------------------------------------------------
# Grafana
# ------------------------------------------------------------------------------

resource "azurerm_container_group" "grafana" {
  name                = "${var.resource_prefix}-grafana"
  location            = var.location
  resource_group_name = var.resource_group_name
  os_type             = "Linux"
  ip_address_type     = "Public"
  dns_name_label      = "${var.resource_prefix}-grafana"

  container {
    name   = "grafana"
    image  = "grafana/grafana:latest"
    cpu    = "1"
    memory = "2"

    ports {
      port     = 3000
      protocol = "TCP"
    }

    environment_variables = {
      GF_SECURITY_ADMIN_PASSWORD = var.grafana_admin_password
      GF_INSTALL_PLUGINS         = "grafana-clock-panel,grafana-piechart-panel"
    }
  }

  tags = var.tags
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "prometheus_url" {
  value = "http://${azurerm_container_group.prometheus.fqdn}:9090"
}

output "loki_url" {
  value = "http://${azurerm_container_group.loki.fqdn}:3100"
}

output "grafana_url" {
  value = "http://${azurerm_container_group.grafana.fqdn}:3000"
}

