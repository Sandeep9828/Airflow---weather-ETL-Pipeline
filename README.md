# Weather ETL Pipeline with Apache Airflow 

##  Project Overview
This project demonstrates an **ETL (Extract, Transform, Load) pipeline** built using **Apache Airflow**.  
It fetches live weather data from the **Open-Meteo API**, transforms it into a structured format, and loads it into a **PostgreSQL database** for further analysis and reporting.  

The project is containerized using **Docker** with **Astro Runtime**, ensuring reproducibility and portability.

---

##  Tech Stack
- **Apache Airflow** (DAG orchestration, task management)  
- **PostgreSQL** (data storage)  
- **Python** (data extraction & transformation logic)  
- **Docker & Astronomer Runtime** (containerized setup)  
- **Open-Meteo API** (weather data source)  

---

##  Features
- Extracts real-time weather data using `HttpHook` from the **Open-Meteo API**.  
- Transforms JSON response into a tabular format with metrics like:
  - Temperature 
  - Windspeed 
  - Wind direction 
  - Weather code 
- Loads structured data into a **Postgres table (`weather_data`)**.  
- Ensures **idempotent execution** (avoiding duplicate data inserts).  
- Supports **DAG scheduling** (daily execution).  

---

## Project Structure