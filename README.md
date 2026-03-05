# Telecom Data Warehouse ETL Project

Production-ready Python ETL pipeline designed for a telecom data warehouse, fully Dockerized and orchestrated with Airflow. Built for high-quality, scalable, and maintainable data engineering workflows.

---

## Key Features & Highlights

- 📂 **Project Structure:** Clean modular design with `src/` for Python ETL scripts, `dags/` for Airflow orchestration, `data/` for raw CSVs, `tests/` for unit tests, and full CI/CD integration via GitHub Actions.

- 🔄 **ETL Pipeline:** Extract → Transform → Load workflow handling customers, SIM cards, and transactions data. Automated data cleaning, validation, and transformation with Python.

- ⭐ **Star Schema Data Warehouse:** Central fact table (`fact_transactions`) connected to dimension tables (`dim_customers`, `dim_sim_cards`, `dim_date`) for efficient analytics.

- 🚀 **Airflow Orchestration:** DAGs automate ETL runs, scheduling, and logging for reliable pipeline execution.

- 🐳 **Dockerized Environment:** Fully containerized pipeline for seamless deployment and environment consistency.

- 🧪 **Unit Testing & CI/CD:** Pytest coverage ensures data integrity; GitHub Actions pipeline handles linting, testing, and deployment.

- 💡 **Pro-Level Data Engineering Skills:** Demonstrates pipeline design, modular coding, orchestration, testing, and cloud-ready deployment.

---

## Project Structure & Visual Overview

Here is a visual overview of the project structure, ETL pipeline, and data warehouse star schema:

<div align="center">
  <table>
    <tr>
      <td align="center">
        <img src="images/1.png" alt="Project Structure" width="300"/><br>
        <em>Project folder and file structure</em>
      </td>
      <td align="center">
        <img src="images/2.png" alt="ETL Pipeline" width="300"/><br>
        <em>ETL Pipeline Flow: Extract → Transform → Load</em>
      </td>
    </tr>
    <tr>
      <td colspan="2" align="center">
        <img src="images/3.png" alt="Star Schema" width="500"/><br>
        <em>Star Schema Data Warehouse: Fact and Dimension Tables</em>
      </td>
    </tr>
  </table>
</div>

---

## Outcome

A fully functional, professional-grade telecom data pipeline ready for production and scalable analytics. Perfect showcase for a **data engineer role**.

---

## Tech Stack

- **Python 3.10** for ETL scripts  
- **Docker & Docker Compose** for environment consistency  
- **Apache Airflow** for orchestration  
- **PostgreSQL** for data warehouse  
- **GitHub Actions** for CI/CD  
- **Pytest** for unit testing  
- **CSV files** for raw telecom data  

---

## How to Run

```bash
# Build and start Docker containers
docker-compose up --build

# Run Airflow webserver
docker-compose exec airflow airflow webserver

# Run Airflow scheduler
docker-compose exec airflow airflow scheduler

# Run tests
pytest tests/