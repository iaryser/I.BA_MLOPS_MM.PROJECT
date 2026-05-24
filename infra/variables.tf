variable "gcp_project_id" {
  description = "Google Cloud project ID"
  type        = string
  default     = "<Your GC project ID"
}

variable "gcp_region" {
  description = "Google Cloud region"
  type        = string
  default     = "europe-west6"
}

variable "gcs_bucket_name" {
  description = "Google Cloud Storage bucket name"
  type        = string
  default     = "<Your unique bucket name>"
}

variable "gcs_location" {
  description = "Google Cloud Storage bucket location"
  type        = string
  default     = "EU"
}

variable "github_owner" {
  description = "GitHub username or organization"
  type        = string
  default     = "<Your Github username>"
}

variable "github_repository" {
  description = "GitHub repository name"
  type        = string
  default     = "<Your repository Name>"
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
  default     = "<Your W&B Entity>"
}

variable "wandb_project" {
  description = "Weights & Biases project"
  type        = string
  default     = "<Your W&B project name>"
}