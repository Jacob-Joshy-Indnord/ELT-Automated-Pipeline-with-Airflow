# Online Sales ELT Automated Pipeline
This project implements a containerized ELT (Extract, Load, Transform) pipeline for online sales data using Apache Airflow, dbt, PostgreSQL, and Apache Superset.

## **Project Overview**
This project demonstrates a production-style Data Engineering pipeline using modern data stack.
It ingests online sales data from Kaggle, orchestrates extraction, loading and transforms with Apache Airflow, transform 
the raw data into analystics-ready models within a dedicated data mart schema using dbt, and serves insights through interactive Apache Superset dashboards

**Goal:** Simulate a real-world ELT pipeline for an e-commerce company to enable better reporting and decision-making.

## **Data Source**
Online Sales data from Kaggle : [Online Sales Dataset](https://www.kaggle.com/datasets/yusufdelikkaya/online-sales-dataset/data)



## **Architecture Overview**

<img width="780" height="541" alt="Image" src="https://github.com/user-attachments/assets/36369feb-d13d-446c-9519-389715c8619a" />

- **Data Source:** CSV file from Kaggle (simulating daily updates; in a real scenario, data would be pulled from an API each day)
- **Orchestration:** Apache Airflow
- **Ingestion:** Python script loads CSV into PostgreSQL (`staging` schema)
- **Transformation:** dbt models create marts in PostgreSQL
- **Visualization:** Apache Superset dashboards
- **Containerization:** All components run in Docker containers via `docker-compose`

## **Pipeline Flow**
1. **Daily Data Update:**  
   The pipeline is scheduled to run every day, simulating an automated daily data pull from an API (currently, the CSV is updated manually to mimic this process).
2. **Extract & Load:**  
   Airflow triggers a Python script to load the latest CSV data into PostgreSQL (`staging.online_sales_data`).
3. **Transform:**  
   Airflow triggers dbt to transform and aggregate data into reporting tables (`mart` schema).
4. **Visualize:**  
   Superset connects to PostgreSQL for interactive dashboards and reporting.

## **Tech Stack**
- Python → Data extraction & loading (Pandas + psycopg2)
- Postgres → Data warehouse (raw + analytics schemas)
- dbt → SQL transformations, models, lineage, data quality tests
- Apache Airflow → Workflow orchestration, scheduling, monitoring
- Docker Compose → Containerized setup for reproducibility
- Apache Superset → Interactive dashboards & BI visualization

## **Features**
- End-to-end ELT pipeline with modern tools
- Orchestrated with Airflow (daily schedule + retries)
- dbt lineage & tests for data quality
- Dockerized for easy setup (1 command to start all services)
- BI dashboard for stakeholders

## **Dashboard**
<img width="1892" height="934" alt="Image" src="https://github.com/user-attachments/assets/b5e7c19d-fa23-4412-b416-6c672d6aa2ae" />

## **Getting Started**

### **Prerequisites**
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

### **Setup Steps**
1. **Clone the repository:**
    ```sh
    git clone https://github.com/natsi02/ELT-Automated-Pipeline-with-Airflow-dbt-PostgreSQL.git
    ```

2. **Configure your data source in `load_data.py`:**
    - By default, the pipeline extracts and loads data from a CSV file (`data/raw/online_sales_dataset.csv`).
    - **Alternatively, you can modify `load_data.py` to pull data from an API source instead of a CSV file.**

3. **Start the pipeline:**
    ```sh
    docker-compose up --build
    ```
    
4. **Access the services:**
    - **Airflow UI:** [http://localhost:8080](http://localhost:8080)
    - **Superset UI:** [http://localhost:8088](http://localhost:8088)
    - **PostgreSQL:** `localhost:5432` (see `profiles.yml` for credentials)

## **Usage**

- Trigger the Airflow DAG `online-sales-ingest-dbt-orchestrator` to run the full pipeline.
- Explore and build dashboards in Superset using the transformed data.

## Orchestration
The pipeline is orchestrated using an Apache Airflow DAG (`airflow/dags/orchestrator.py`).
- **Task 1:** Runs the Python script to ingest data from the data source (CSV or API) into PostgreSQL.
- **Task 2:** Runs dbt in a Docker container to transform and aggregate the data.
- Airflow ensures these tasks run in sequence and can be scheduled to run automatically (e.g., daily).

## New sales source (provisional)
A second feed lands in `landing_new.*` (`request-script/load_new_source.py`), is combined with the legacy feed in
`stg_combined_sales` upstream of `sales_report`, and is gated by dbt tests and a daily cutoff. See `docs/new_source_release.md`.
Run the Python tests with `pytest tests`.

## **Customization**

- **dbt models:** Modify or add models in `dbt/my_project/models/` for new marts or transformations.
- **Airflow DAG:** Edit `airflow/dags/orchestrator.py` to change scheduling or add tasks.
- **Superset:** Create new dashboards or charts as needed.

## **Acknowledgements**
- [Kaggle](https://www.kaggle.com/) for the dataset
- [dbt Labs](https://www.getdbt.com/)
- [Apache Airflow](https://airflow.apache.org/)
- [Apache Superset](https://superset.apache.org/)
- **My sincere thanks to [Calvin Yoon Channel](https://www.youtube.com/@cyprojects/) for providing such a valuable resource and inspiration.**
