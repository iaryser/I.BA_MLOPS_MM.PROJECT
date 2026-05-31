# Cryptocurrency Price Direction Prediction System

This repository contains a cloud-ready MLOps system for predicting the short-term price direction of cryptocurrencies. It uses dynamic market data from the CoinGecko API, computes time-series features for a configurable coin universe, trains an XGBoost classifier, tracks model artifacts with Weights & Biases, and serves predictions through a deployed FastAPI and Streamlit application.

The project was developed for the **I.BA_MLOPS_MM.F2601 Machine Learning Operations** module at HSLU during Spring Semester 2026.

For a short project summary, see [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md).

Also, make sure to check out the deployed application: [https://crypto-prediction-frontend-200591620097.europe-west6.run.app/](https://crypto-prediction-frontend-200591620097.europe-west6.run.app/)

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

## Data and Features

Raw market data is fetched from the **CoinGecko API**. The pipeline uses price, market capitalization, trading volume, timestamps, and coin identifiers to build time-series features.

Generated features include several groups of time-series indicators:

| Feature group | Description |
|---|---|
| Returns | Short-term percentage price changes over multiple lookback windows |
| Moving-average deviations | Distance between the current price and recent rolling averages |
| Volatility | Rolling standard deviation of recent returns |
| Normalized momentum | Return-based momentum scaled by recent volatility |
| Volume dynamics | Log-volume changes over multiple lookback windows |
| Liquidity ratio | Trading volume normalized by market capitalization |
| Target | Binary label indicating whether the future price increased |

The offline feature table is used for model training. The online feature table stores the latest feature row per coin for low-latency inference.

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
- Terraform >= 1.6
- Google Cloud CLI
- GitHub CLI
- A Google Cloud project with billing enabled 
- A CoinGecko API key [https://www.coingecko.com/en/api/pricing](https://www.coingecko.com/en/api/pricing)
- A Weights & Biases account and API key [https://docs.wandb.ai/models/quickstart](https://docs.wandb.ai/models/quickstart)

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

### 2. Configure Terraform variables

Go into the infrastructure folder:

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
```

Fill in your own values.

To get a CoinGecko API key, register on: 
[https://www.coingecko.com/en/api/pricing](https://www.coingecko.com/en/api/pricing)

To get a Weights & Biases API key, register on:
[https://docs.wandb.ai/models/quickstart](https://docs.wandb.ai/models/quickstart)

You will need to create an account for this.

---

### 3. Authenticate locally

Authenticate with Google Cloud:

```bash
gcloud auth application-default login
gcloud config set project <your-gcp-project-id>
```

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
---

### 4. Apply Terraform


```bash
terraform init
terraform plan
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

---

### 5. Run the workflows

After setting up Terraform, run the workflows in this order:

1. **Feature Backfill**
   - creates the initial historical feature store
   - required before training and inference

2. **Train XGBoost Classifier Model**
   - trains and registers the first model in W&B

3. **Batch Feature Refresh**
   - updates the online feature table with current features

   After this you could already run the api & streamlit app locally with:

   ```bash
   docker compose up --build -d
   ```

4. **Deploy Cloud Run**
   - deploys the FastAPI backend and Streamlit frontend

---

## Author

**Timo Ryser**  
Machine Learning Operations Project  
HSLU, Spring Semester 2026
