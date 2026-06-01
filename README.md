# Cryptocurrency Price Direction Prediction System

This repository contains a cloud-ready MLOps system for predicting the short-term price direction of cryptocurrencies. 

The project was developed for the **I.BA_MLOPS_MM.F2601 Machine Learning Operations** module at HSLU during Spring Semester 2026.

For a short project summary, click [here](docs/PROJECT_SUMMARY.md).

Also, make sure to check out the Deployed Application: 
[https://crypto-prediction-frontend-200591620097.europe-west6.run.app/](https://crypto-prediction-frontend-200591620097.europe-west6.run.app/)

---

## Tech Stack

| Area | Technology |
|---|---|
| Data source | CoinGecko API |
| Data processing | Python, Pandas |
| Feature storage | Parquet files on Google Cloud Storage |
| Model training | XGBoost, scikit-learn |
| Experiment tracking | Weights & Biases |
| Model registry | Weights & Biases Artifacts |
| Backend | FastAPI |
| Frontend | Streamlit |
| Containerization | Docker |
| Dependency management | uv |
| Infrastructure | Terraform, Google Cloud |
| Deployment | Google Cloud Run, Artifact Registry |
| Automation | GitHub Actions |
| CI | Ruff, pytest, Docker image builds |

---

## Repository Structure

```text
.
├── .github/workflows/
│   ├── ci.yaml                   # linting, tests, Docker builds
│   ├── batch_features.yaml       # hourly feature refresh
│   ├── backfill_features.yaml    # manual historical backfill
│   ├── train_xgboost.yaml        # scheduled/manual model training
│   └── deploy_cloud_run.yaml     # deploy API and frontend to Cloud Run
│
├── data/reference/
│   └── top100_coins.json         # configured coin universe
│
├── frontend/
│   └── app.py                    # Streamlit frontend
│
├── infra/
│   ├── main.tf                   # GCP + GitHub infrastructure
│   ├── variables.tf              # Terraform variables
│   ├── outputs.tf                # Terraform outputs
│   └── terraform.tfvars.example  # example secret/config file
│
├── src/
│   ├── feature_pipeline/         # ingestion, staging, feature generation
│   ├── training_pipeline/        # dataset building and model training
│   └── inference/                # FastAPI service and inference logic
│
├── tests/unit/                   # unit tests
├── Dockerfile.api                # backend container
├── Dockerfile.frontend           # frontend container
├── pyproject.toml                # dependencies and project scripts
├── uv.lock                       # locked dependencies
└── README.md
```

---


## Automation Workflows

The project uses GitHub Actions to automate testing, feature generation, model training and deployment.

| Workflow | Trigger | Purpose |
|---|---|---|
| CI | Push / pull request | Builds Docker images, installs dependencies, runs Ruff and pytest |
| Feature Backfill | Manual | Creates historical raw data, staging data, offline features, and online features |
| Batch Feature Refresh | Scheduled hourly / manual | Fetches fresh CoinGecko data and updates the feature store |
| Train XGBoost | Scheduled weekly / manual | Trains the model from the offline feature store and logs metrics/artifacts to W&B |
| Deploy Cloud Run | Manual | Builds and deploys the FastAPI backend and Streamlit frontend to Google Cloud Run |

---

## Setup

### Prerequisites

Install the following tools locally: 

- Python 3.14
- uv
- Docker
- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.6 
- [Google Cloud CLI](https://docs.cloud.google.com/sdk/docs/install-sdk?hl=de)
- [GitHub CLI](https://cli.github.com/)

You will also need:

- A Google Cloud account with billing enabled
- A [CoinGecko](https://www.coingecko.com/en/api/pricing) API key
- A [Weights & Biases](https://docs.wandb.ai/models/quickstart) account and API key 

---

## Reproducing the Full System

To reproduce this project, create your own fork or repository copy and configure it with your own Google Cloud project, CoinGecko API key, and Weights & Biases account.

A plain local clone is not enough for the full setup, because the GitHub Actions workflows depend on repository-specific secrets, variables, and Google Cloud Workload Identity Federation.

---

### 1. Fork or copy the repository

Create your own copy of this repository on GitHub.

Then clone your repository locally:

```bash
git clone https://github.com/<your-github-owner>/<your-repository-name>.git
cd <your-repository-name>
```
--- 

### 2. Create a Google Cloud project

Create a new Google Cloud project and enable billing for it.

Note down the project ID, because you will need it for `terraform.tfvars`.

The easiest setup is to use a project where your Google account has Owner permissions.

Authenticate locally and set the active project:

```bash
gcloud auth application-default login
gcloud config set project <your-gcp-project-id>
```

---

### 3. Configure Terraform variables

Go into the infrastructure folder and create your local Terraform variable file:

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
```

Open `terraform.tfvars` and replace the placeholder values with your own configuration:

| Variable | Description |
|---|---|
| `gcp_project_id` | Your Google Cloud project ID |
| `gcs_bucket_name` | A globally unique Google Cloud Storage bucket name, for example `<your-project-id>-crypto-mlops-data` |
| `github_owner` | Your GitHub username or organization name |
| `github_repository` | The repository name only, not the full GitHub URL |
| `wandb_entity` | Your Weights & Biases username or team name |
| `wandb_project` | The Weights & Biases project name used for experiment tracking and model artifacts |
| `coingecko_api_key` | Your CoinGecko API key |
| `wandb_api_key` | Your Weights & Biases API key |


You can get a CoinGecko API key from [CoinGecko](https://www.coingecko.com/en/api/pricing).

You can get a Weights & Biases API key from [Weights & Biases](https://docs.wandb.ai/models/quickstart).

---

### 4. Authenticate with GitHub

Authenticate with GitHub CLI:

```bash
gh auth login
```
Terraform needs a GitHub token to create repository variables and secrets. If you are authenticated with GitHub CLI, expose the token for the current shell.

**macOS / Linux / Git Bash**
```bash
export GITHUB_TOKEN=$(gh auth token)
```

**Windows PowerShell**
```powershell
$env:GITHUB_TOKEN = gh auth token
```

For private repositories, make sure your GitHub CLI authentication has permission to manage repository Actions secrets and variables. If needed, refresh the token scopes with:

```bash
gh auth refresh -s repo
```

---

### 5. Apply Terraform

**Initialize terraform**

```bash
terraform init
```

**Preview the infrastructure changes**

```bash
terraform plan
```

**Apply the infrastructure changes**

```bash
terraform apply
```

Terraform creates the required cloud and repository configuration:

- Google Cloud Storage bucket
- Artifact Registry Docker repository
- GitHub Actions service account
- Workload Identity Federation for GitHub Actions
- GitHub Actions variables
- GitHub Actions secrets
- Secret Manager secret for the W&B API key
- required Google Cloud APIs

It will also create a `.env` file.

---

### 6. Run the workflows

After Terraform has finished, open your GitHub repository and go to the **Actions** tab.

Run the workflows in this order:

1. **Feature Backfill**
   - Run this workflow manually first.
   - It creates the initial historical raw data, staging table, offline feature table, and online feature table.
   - This is required before model training and inference can work.

2. **Train XGBoost Classifier Model**
   - Run this workflow manually after the backfill has completed.
   - It trains the first model and registers the production model artifact in Weights & Biases.

3. **Deploy Cloud Run**
   - Run this workflow manually after the first model has been registered.
   - It builds and deploys the FastAPI backend and Streamlit frontend to Google Cloud Run.

4. **Batch Feature Refresh**
   - This workflow keeps the feature store up to date.
   - It can be run manually, but it is mainly intended to run on its hourly schedule.

The scheduled workflows only run if GitHub Actions are enabled for the repository. If GitHub shows a workflow as disabled, enable it from the **Actions** tab. 

After the backfill and first training run have completed, you can also run the API and Streamlit app locally. See [Local Development](#local-development).

---

## Local Development

Install dependencies:

```bash
uv sync --dev
```

Run quality checks and tests:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

Available project commands:

```bash
uv run update-coin-universe
uv run feature-backfill --n-coins 100 --currency chf --n-days 365
uv run feature-batch --n-coins 100
uv run train-xgboost
```

### Local Docker Compose note

The included `docker-compose.yaml` is configured for Windows because it mounts the local Google application-default credentials file from:

```text
${APPDATA}/gcloud/application_default_credentials.json
```

If you are using Linux or macOS, replace that volume mount with:

```yaml
- ${HOME}/.config/gcloud/application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json:ro
```

Before running the containers locally, authenticate once with Google Cloud (if not done already):

```bash
gcloud auth application-default login
```

Then start the API and frontend:

```bash
docker compose up --build -d
```

Local URLs:

- FastAPI docs: http://localhost:8000/docs
- API health check: http://localhost:8000/health
- Streamlit frontend: http://localhost:8501


## Author

**Timo Ryser**  
Machine Learning Operations Project  
HSLU, Spring Semester 2026
