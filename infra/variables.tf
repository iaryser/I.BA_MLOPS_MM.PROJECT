variable "gcp_project_id" {
  description = "Google Cloud project ID"
  type        = string
  default     = "mlops-496411"
}

variable "gcp_region" {
  description = "Google Cloud region"
  type        = string
  default     = "europe-west6"
}

variable "gcs_bucket_name" {
  description = "Google Cloud Storage bucket name"
  type        = string
  default     = "mlops-496411-bucket"
}

variable "gcs_location" {
  description = "Google Cloud Storage bucket location"
  type        = string
  default     = "EU"
}

variable "github_owner" {
  description = "GitHub username or organization"
  type        = string
  default     = "iaryser"
}

variable "github_repository" {
  description = "GitHub repository name"
  type        = string
  default     = "I.BA_MLOPS_MM.PROJECT"
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
  default     = "hslu-DSPRO2"
}

variable "wandb_project" {
  description = "Weights & Biases project"
  type        = string
  default     = "crypto-direction-prediction"
}