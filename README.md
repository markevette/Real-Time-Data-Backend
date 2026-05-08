# Real-Time Data Backend for Streaming Taxpayer Metrics  
DLMDSEDE02 – Data Engineering Portfolio Project  
Author: Evette Mark  

## 1. Project Overview  
This project implements a real-time data backend designed to ingest, process, and aggregate
large volumes of German taxpayer–related records. The system simulates a continuous data
stream using a time-stamped dataset (e.g., income tax filings, wage tax records, or household
tax contributions). The goal is to provide low-latency, continuously updated metrics suitable
for a real-time reporting dashboard.

The architecture follows a microservice-based design using Docker, Kafka, Spark Structured
Streaming, PostgreSQL, and FastAPI. All components run in isolated containers to ensure
reproducibility, maintainability, and alignment with modern data engineering practices.

---

## 2. Architecture  
The system consists of the following microservices:

- **Producer Service**  
  Streams German taxpayer records row-by-row into Kafka to emulate real-time ingestion.

- **Kafka Cluster**  
  Provides durable, scalable log storage and decouples producers from consumers.

- **Spark Structured Streaming**  
  Performs real-time preprocessing, cleaning, and event-time windowed aggregations  
  (e.g., average taxable income per minute, rolling count of filings, etc.).

- **PostgreSQL Database**  
  Stores aggregated metrics for downstream consumption.

- **FastAPI Service**  
  Exposes REST endpoints for dashboards or analytical tools.

All services are orchestrated using `docker-compose`.

---

## 3. Dataset  
The system is designed for datasets containing:

- taxpayer ID or anonymized identifier  
- timestamp (e.g., filing date, processing time, wage payment date)  
- taxable income  
- tax class (Steuerklasse)  
- deductions, contributions, or other relevant attributes  

Example datasets include:  
- *German Income Tax Dataset (Kaggle)*  
- *Einkommensteuerstatistik (public microdata)*  
- *Synthetic wage tax datasets for educational use*

The producer service reads the dataset sequentially and publishes each row as a Kafka event.

---

## 4. How to Run the System  

### Prerequisites  
- Docker & Docker Compose  
- Python 3.10+ (optional for local testing)

### Start the full pipeline  
```bash
docker-compose up --build
