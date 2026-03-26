# 💊 Pharmaceutical Supply Chain Data Pipeline

## 🎯 Overview
This project implements a modern **ELT data pipeline** using dbt, BigQuery, and Looker Studio to process and analyze pharmaceutical supply chain data. It focuses on data cleaning, transformation, validation, and visualization for business insights.

---

## 🧰 Tech Stack
- **Python** (pandas, faker) – data generation & preprocessing  
- **dbt** – data transformation, modeling, testing  
- **Google BigQuery** – cloud data warehouse  
- **Looker Studio** – dashboard & visualization  
- **GitHub** – version control  

---

## 📁 Project Structure
```text
pharma_poc/
├── models/                  # dbt models (staging, marts)
├── data/                    # raw datasets
├── enrich_pii_dataset.py    # data generation & enrichment script
├── packages.yml             # dbt packages
├── README.md
```

## 🔑 Key Features
- Implemented **PII masking** (Aadhaar, DOB, Phone, Email) for data privacy  
- Built **modular dbt models** for scalable data transformations  
- Created **derived metrics** such as `delivery_lag` and `value_per_kg`  
- Performed **data validation and testing** using dbt tests and dbt-utils  
- Designed **interactive dashboards** in Looker Studio for analytics

## 🚀 How to Reproduce
1. Clone the repository  
2. Upload `SCMS_with_PII.csv` to BigQuery  
3. Configure `profiles.yml` and service account  
4. Execute:

```bash
dbt run
dbt test
dbt docs generate
dbt docs serve
