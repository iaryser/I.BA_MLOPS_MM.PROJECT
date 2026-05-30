variable "gcp_project_id" {
  description = "Google Cloud project ID"
  type        = string
}

variable "gcp_region" {
  description = "Google Cloud region"
  type        = string
  default     = "europe-west6"
}

variable "gcs_bucket_name" {
  description = "Globaly unique Google Cloud Storage bucket name "
  type        = string
}

variable "gcs_location" {
  description = "Google Cloud Storage bucket location"
  type        = string
  default     = "EU"
}

variable "github_owner" {
  description = "GitHub username or organization"
  type        = string
}

variable "github_repository" {
  description = "GitHub repository name"
  type        = string
}

variable "coingecko_api_key" {
  description = "CoinGecko API key"
  type        = string
  sensitive   = true
}

variable "wandb_api_key" {
  description = "Weights & Biases API key"
  type        = string
  sensitive   = true
}

variable "wandb_entity" {
  description = "Weights & Biases entity"
  type        = string
}

variable "wandb_project" {
  description = "Weights & Biases project"
  type        = string
}