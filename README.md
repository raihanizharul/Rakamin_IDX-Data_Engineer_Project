# Data Warehouse Project
  
One of the clients of ID/X Partners, operating in the banking industry, required the development of a Data Warehouse to integrate multiple data sources stored within their internal systems. The client manages data originating from several many sources including Excel files, CSV documents, and Databases. The primary challenge faced by the client is the inability to extract and consolidate data from multiple sources simultaneously. This limitation leads to delayed reporting cycles and obstructs timely data analysis, which is critical for operations and decision-making within the banking environment.

This project focuses on building a robust Data Warehouse solution using the Medallion Architecture (Bronze, Silver, Gold layers). Additionally, the project implements stored procedures to generate Daily Transactions and Customer Balance outputs.

---
## 🛠️ Data Architecture
![Data Architecture](docs/data_architecture.png)

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV, Excel, and Databases into SQL Server Database. In this layer light transformations are applied, including data type standardization, to ensure consistent data formats before the data is processed in the silver layers.

2. **Silver Layer**: This layer includes data cleansing and data enrichment processes to prepare data for analysis. Data cleansing activities typically involve removing duplicates, standardizing column values such as converting them to uppercase, and applying sorting rules to organize the dataset. Data enrichment focuses on combining and integrating related transactional data into a unified and well-structured dataset, enabling more meaningful analysis in the subsequent layer.

3. **Gold Layer**: Houses business-ready data modeled into a [Star Schema](docs/ERD_StarSchema.png) required for reporting and analytics. 

## 🛠️ Data Flow
![Data Flow](docs/data_flow.png)

---
🗂️ Project Structure
```
data-warehouse-project/
│
├── Airflow                             # Run project in this folder
│
├── datasets/                           # Raw datasets used for the project (Excel, CSV, Database (Restore .bak))
│
├── docs/                               # Project documentation and architecture details
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── ddl/                            # Scripts for init data warehouse and layers
│   ├── store_procedure/                # Scripts for store procedure
│
├── README.md                           # Project overview and instructions
```
---
##  🚀 How to Run the Project

To run this project locally, follow the steps below:

### 1️⃣ Install Required Tools

Make sure the following dependencies are installed on your machine:
- Docker
- Astro CLI for running Apache Airflow
- SQL Server Express
- SQL Server Management Studio (SSMS) for managing the database

### 2️⃣ Restore the Sample Database
- Open SSMS and connect to your local SQL Server Express instance.
- Right-click on Databases → Restore Database
- Select the backup file located in datasets/sample.bak

### 3️⃣ Start the Airflow Environment

- Open a terminal or command prompt
- Navigate to the Airflow directory of the project:
```
cd data-warehouse-project/Airflow
```
- Start the local Airflow environment using the Astro CLI:
```
astro dev start
```
---
View project presentation click [here](https://youtu.be/iGD4w4DHFJA?si=VAdT7p3puASaJNST)