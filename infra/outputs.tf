output "gcs_bucket_name" {
  description = "Created GCS bucket name"
  value       = google_storage_bucket.mlops_bucket.name
}

output "github_actions_service_account" {
  description = "Service account used by GitHub Actions"
  value       = google_service_account.github_actions.email
}

output "workload_identity_provider" {
  description = "Workload Identity Provider resource name"
  value       = google_iam_workload_identity_pool_provider.github_provider.name
}