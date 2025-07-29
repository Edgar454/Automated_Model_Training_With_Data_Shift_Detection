# 🌧️ Automated Localised Precipitation Forecasting in Brazzaville  
### Powered by MLflow, Airflow, DVC, and Drift Detection

Accurate precipitation forecasting is essential for effective urban planning, agriculture, water management, and disaster mitigation. In Brazzaville, the capital of the Republic of Congo, rainfall patterns are highly variable—often leading to flash floods, crop damage, and strain on public infrastructure. However, the lack of high-resolution and locally-tuned forecasting tools remains a significant challenge, particularly in many sub-Saharan African cities where data and computational resources are limited.

This project aims to bridge that gap by building a fully automated, localized rainfall prediction system tailored for Brazzaville, leveraging modern MLOps tools and data drift monitoring capabilities.

---

## 🏗️ Project Architecture

The system integrates several technologies to ensure continuous, reliable, and explainable forecasts:

- **MLflow**: Model registry and experiment tracking (hyperparameters, metrics, artifacts).
- **Apache Airflow**: Workflow orchestration for the full ML pipeline (data ingestion, validation, retraining).
- **DVC (Data Version Control)**: Handles dataset versioning, reproducibility, and pipeline tracking.
- **Evidently AI**: Integrated in Airflow DAGs to monitor for data and concept drift.
- **Streamlit**: User-facing web interface for visualizing predictions and model status.
- **GitHub Actions**: Continuous Integration for code testing and image building.
- **Kubernetes**: Container orchestration and production deployment.

---

## 🚀 Key Features

- **End-to-end MLOps pipeline** with auto-retraining triggered by real-world data drift  
- **Model registry and experiment versioning** using MLflow  
- **Data reproducibility and tracking** with DVC  
- **Interactive UI** to access local weather insights via Streamlit  
- **CI/CD automation** using GitHub Actions and Kubernetes  
- **Monitoring system** for real-time alerting on data and concept drift  

---

## 📦 Current Progress

| Component                | Status         |
|--------------------------|----------------|
| Data ingestion pipeline  | 🏗️ In progress  |
| Model training pipeline  | ⏳ Coming soon  |
| Drift detection pipeline | ⏳ Coming soon  |
| Streamlit UI             | ⏳ Coming soon  |
| DVC integration          | ✅ Completed    |
| Containerization         | ⏳ Coming soon  |
| CI with GitHub Actions   | ⏳ Coming soon  |
| Deployment on Kubernetes | ⏳ Coming soon  |
| Monitoring & logging     | ⏳ Coming soon  |

---

## 🛠️ Next Steps

- Finalize data ingestion and DVC tracking  
- Implement drift detection and training DAG in Airflow  
- Build the Streamlit forecasting dashboard  
- Containerize all services for Kubernetes  
- Set up GitHub Actions workflows for CI  
- Enable continuous deployment to Kubernetes cluster  

---

## 🤝 Contributions

This is a work-in-progress project. Feedback, ideas, or contributions are welcome!  
Please open an issue or submit a pull request if you'd like to contribute.

---

