# 🎬 Movie Analytics Engineering Pipeline (Airflow + Postgres + dbt)

## 📌 Overview

This project demonstrates a production-style data pipeline using:
- Apache Airflow (orchestration)
- PostgreSQL (data storage)
- TMDB API (data source)
- dbt (planned transformation layer)

The pipeline follows a Medallion Architecture (Bronze → Silver → Gold).

---

## 🏗️ Architecture

TMDB API → Airflow DAG → Postgres (Bronze Layer) → dbt (Silver & Gold Layers)

---

## 🧱 Tech Stack

- Python
- Apache Airflow 2.7
- PostgreSQL 15
- Docker & Docker Compose
- dbt (next phase)

---

## 📂 Project Structure

.
├── docker-compose.yml
├── airflow/
│   └── dags/
│       └── movie_ingestion_dag.py
├── ingestion.py
├── .env
├── requirements.txt

---

## 🚀 Setup Instructions

### 1. Clone Repository

git clone <repo-url>
cd <repo>

---

### 2. Start Services

docker-compose up -d

---

### 3. Access Airflow UI

URL: http://localhost:8080  
Username: admin  
Password: admin  

---

### 4. Trigger DAG

- DAG Name: movie_ingestion  
- Click → Trigger DAG  

---

## 🗄️ Database Design (Bronze Layer)

CREATE SCHEMA bronze;

CREATE TABLE bronze.raw_movies (
    movie_id INT PRIMARY KEY,
    title TEXT,
    popularity FLOAT,
    vote_count INT,
    vote_average FLOAT,
    release_date DATE,
    original_language TEXT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

---

## 🔄 Data Ingestion Logic

- Source: TMDB API
- Fetch using Python requests
- Insert using psycopg2
- Duplicate handling:

ON CONFLICT (movie_id) DO NOTHING;

---

## ⚙️ Airflow DAG

- Operator: PythonOperator
- Task: fetch_movies
- Steps:
  1. Call API
  2. Parse JSON
  3. Insert into Postgres Bronze layer

---

## ⚠️ Challenges Faced & Fixes

Docker networking issue  
→ Fixed by using single docker-compose and service names  

Airflow DB connection error  
→ Corrected hostname to airflow_postgres  

Container conflicts  
→ Removed old containers  

502 Airflow error  
→ Fixed configuration and clean restart  

localhost not working inside Docker  
→ Used service name (movie_postgres)  

---

## ✅ Current Status

- Airflow running successfully  
- DAG executed successfully  
- Data inserted into bronze.raw_movies  

---

## 🔜 Next Steps

- Setup dbt  
- Build Silver layer  
- Create Gold layer  
- Add BI tool (Superset / alternative)  

---

## 💡 Key Learnings

- Docker networking fundamentals  
- Airflow debugging and orchestration  
- API ingestion patterns  
- Medallion architecture implementation  
