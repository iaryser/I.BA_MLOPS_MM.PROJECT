terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.6"
    }

    github = {
      source  = "integrations/github"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

provider "github" {
  owner = var.github_owner
}

locals {
  github_repo_full = "${var.github_owner}/${var.github_repository}"
}

data "google_project" "current" {
  project_id = var.gcp_project_id
}

resource "google_project_service" "required_apis" {
  for_each = toset([
    "storage.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "sts.googleapis.com",
  ])

  project            = var.gcp_project_id
  service            = each.key
  disable_on_destroy = false
}

resource "google_storage_bucket" "mlops_bucket" {
  name                        = var.gcs_bucket_name
  location                    = var.gcs_location
  uniform_bucket_level_access = true
  force_destroy               = true

  versioning {
    enabled = true
  }

  depends_on = [
    google_project_service.required_apis
  ]
}

resource "google_service_account" "github_actions" {
  account_id   = "github-actions-mlops"
  display_name = "GitHub Actions MLOps Service Account"

  depends_on = [
    google_project_service.required_apis
  ]
}

resource "google_storage_bucket_iam_member" "github_actions_bucket_access" {
  bucket = google_storage_bucket.mlops_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.github_actions.email}"
}

resource "google_iam_workload_identity_pool" "github_pool" {
  workload_identity_pool_id = "github-actions-pool"
  display_name              = "GitHub Actions Pool"

  depends_on = [
    google_project_service.required_apis
  ]
}

resource "google_iam_workload_identity_pool_provider" "github_provider" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-actions-provider"
  display_name                       = "GitHub Actions Provider"

  attribute_mapping = {
    "google.subject"             = "assertion.sub"
    "attribute.repository"       = "assertion.repository"
    "attribute.repository_owner" = "assertion.repository_owner"
    "attribute.ref"              = "assertion.ref"
  }

  attribute_condition = "attribute.repository == '${local.github_repo_full}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_member" "github_actions_workload_identity_user" {
  service_account_id = google_service_account.github_actions.name
  role               = "roles/iam.workloadIdentityUser"

  member = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_pool.name}/attribute.repository/${local.github_repo_full}"
}

resource "github_actions_variable" "gcp_project_id" {
  repository    = var.github_repository
  variable_name = "GCP_PROJECT_ID"
  value         = var.gcp_project_id
}

resource "github_actions_variable" "gcp_region" {
  repository    = var.github_repository
  variable_name = "GCP_REGION"
  value         = var.gcp_region
}

resource "github_actions_variable" "gcs_bucket_name" {
  repository    = var.github_repository
  variable_name = "GCS_BUCKET_NAME"
  value         = google_storage_bucket.mlops_bucket.name
}

resource "github_actions_variable" "gcp_service_account" {
  repository    = var.github_repository
  variable_name = "GCP_SERVICE_ACCOUNT"
  value         = google_service_account.github_actions.email
}

resource "github_actions_variable" "gcp_workload_identity_provider" {
  repository    = var.github_repository
  variable_name = "GCP_WORKLOAD_IDENTITY_PROVIDER"
  value         = google_iam_workload_identity_pool_provider.github_provider.name
}

resource "github_actions_variable" "wandb_entity" {
  repository    = var.github_repository
  variable_name = "WANDB_ENTITY"
  value         = var.wandb_entity
}

resource "github_actions_variable" "wandb_project" {
  repository    = var.github_repository
  variable_name = "WANDB_PROJECT"
  value         = var.wandb_project
}

resource "github_actions_secret" "coingecko_api_key" {
  repository  = var.github_repository
  secret_name = "COINGECKO_API_KEY"
  value       = var.coingecko_api_key
}

resource "github_actions_secret" "wandb_api_key" {
  repository  = var.github_repository
  secret_name = "WANDB_API_KEY"
  value       = var.wandb_api_key
}

resource "local_sensitive_file" "env_file" {
  filename = "${path.module}/../.env"

  content = <<EOT
  COINGECKO_API_KEY="${var.coingecko_api_key}"

  WANDB_API_KEY="${var.wandb_api_key}"
  WANDB_ENTITY="${var.wandb_entity}"
  WANDB_PROJECT="${var.wandb_project}"

  ONLINE_FEATURE_PATH="gs://${var.gcs_bucket_name}/online_store/online_features.parquet"
  MARKET_DATA_PATH="gs://${var.gcs_bucket_name}/staging/market_data.parquet"
  EOT
}