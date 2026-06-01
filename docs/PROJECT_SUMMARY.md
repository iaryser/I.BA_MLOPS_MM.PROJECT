# Cryptocurrency Price Direction Prediction System

This project predicts whether cryptocurrency prices will move up or down over a 24-hour horizon using live CoinGecko market data. It follows an automated FTI architecture: GitHub Actions refreshes features, backfills historical data, trains an XGBoost model, and deploys the services to Google Cloud Run. Features are stored as Parquet datasets in Google Cloud Storage, while Weights & Biases tracks experiments and manages the production model. FastAPI serves predictions from the latest online features and model, and Streamlit provides the user-facing prediction dashboard.

<p align="center">
  <img src="architecture.png" alt="Architecture Diagram" width="650">
</p>