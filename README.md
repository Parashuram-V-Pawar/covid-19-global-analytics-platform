# COVID-19 Global Analytics Platform
A distributed big data analytics platform built using **Apache Spark (PySpark)** integrated with **Hadoop (HDFS + YARN)** in pseudo-distributed mode.

This project simulates a real-world Data Engineering use case for analyzing global COVID-19 datasets at scale using distributed processing techniques.

---
## Project Objective
Design, implement, optimize, and analyze a distributed COVID analytics platform where:
- All data is stored in HDFS
- All Spark jobs run on YARN
- Data is processed using:
  - DataFrame API
  - Spark SQL
  - RDD API
- Performance tuning and execution plan analysis are implemented

---
## Tech Stack Used
```
Language: Python
Framework: Apache Spark (PySpark)
Storage: Hadoop HDFS
Resource Manager: YARN
Processing APIs:
    - DataFrame API
    - Spark SQL
    - RDD API
File Format: CSV, Parquet
Tools: VSCode, Jupyter Notebook, Git
```
---
## Hadoop Directory Structure (HDFS)
```
/data/covid/raw
/data/covid/staging
/data/covid/curated
/data/covid/analytics
```
All the outputs are written into /data/covid/analytics
Note: All Spark jobs strictly read from and write to HDFS.

---
## Dataset Used
Kaggle Dataset: https://www.kaggle.com/datasets/imdevskp/corona-virus-report/

### Tables Processed:
- full_grouped.csv
- covid_19_clean_complete.csv
- country_wise_latest.csv
- day_wise.csv
- usa_county_wise.csv
- worldometer_data.csv

---
## Implemented Tasks
- Task 1 – Hadoop Integration
- Task 2 – Data Ingestion & Optimization
- Task 3 – Death Percentage Analysis
- Task 4 – Infection Rate Analysis
- Task 5 – Recovery Efficiency
- Task 6 – Global Time Series Analysis
- Task 7 – USA Drilldown Analysis
- Task 8 – RDD-Based Implementation
- Task 9 – Spark SQL Implementation
- Task 10 – Performance Optimization
- Task 11 – Execution Plan Analysis
- Task 12 – Resource & Memory Planning

---
## Project Structure
```
COVID-19-GLOBAL-ANALYTICS-PLATFORM/

|-- data/
|   |-- country_wise_latest.csv
|   |-- covid_19_clean_complete.csv
|   |-- day_wise.csv
|   |-- full_grouped.csv
|   |-- usa_county_wise.csv
|   |-- worldometer_data.csv
|
|-- setup_scripts/
|   |-- load_input_files.sh
|   |-- setup.sh
|   |-- start-hadoop.sh
|   |-- stop-hadoop.sh
|
|-- scripts/
|   |-- data_ingestion_and_optimization.py
|   |-- death_percentage_analysis.py
|   |-- infection_rate_analysis.py
|   |-- recovery_efficiency.py
|   |-- global_time_series_analysis.py
|   |-- usa_drilldown_analysis.py
|   |-- rdd_based_implementation.py
|   |-- spark_sql_implementation.py
|   |-- performance_optimization.py
|   |-- execution_plan_analysis.py
|   |-- resource_and_memory_planning.txt
|
|-- documentation/
|   |-- hdfs_command_documentation.txt
|   |-- resource_and_memory_planning.txt
|
|-- screenshots/
|   |-- execution_plan_aggregation_query.png
|   |-- execution_plan_join_query.png
|   |-- execution_plan_window_function_querty.png
|
|-- README.md
|-- requirements.txt
|-- .gitignore
```

---
## Installation
```
Clone the repository
-> git clone https://github.com/Parashuram-V-Pawar/covid-19-global-analytics-platform.git

Move to project folder
-> cd covid-19-global-analytics-platform

Install dependencies
-> pip install -r requirements.txt

Make all shell scripts executable
-> chmod +x setup_scripts/*.sh

Now run 'setup.sh' to setup the environment for the project
-> setup_scripts/setup.sh
```

---
## How to Run
### 1. Start Hadoop services
```
setup_scripts/start-hadoop.sh
```
This will start service and list the running services

### 2. Upload Data to HDFS
```
setup_scripts/load_input_files.sh
```

### 3. Submit Spark Job
Note: All jobs must run using:
```
spark-submit --master yarn
```
```
You can find the .py files in scripts which are the jobs to run those
-> spark-submit --master yarn scripts/data_ingestion_and_optimization.py
-> spark-submit --master yarn scripts/death_percentage_analysis.py
-> spark-submit --master yarn scripts/infection_rate_analysis.py
-> spark-submit --master yarn scripts/recovery_efficiency.py
-> spark-submit --master yarn scripts/global_time_series_analysis.py
-> spark-submit --master yarn scripts/usa_drilldown_analysis.py
-> spark-submit --master yarn scripts/rdd_based_implementation.py
-> spark-submit --master yarn scripts/spark_sql_implementation.py
-> spark-submit --master yarn scripts/performance_optimization.py
-> spark-submit --master yarn scripts/execution_plan_analysis.py

Note: Hadoop services should be running to execute these.
```
### 4. Execute on Jupyter Notebook
We also have jupyter notebooks for the same tasks to execute those
```
1. start jupyter notebook session
-> command: jupyter notebook

2. next jupyter notebook UI will open there in scripts/notebook select the task
-> Now you run the cells in order to get the expected results.

Note: Hadoop services should be running to execute these.
```

### 5. Stop Hadoop services
```
setup_scripts/stop-hadoop.sh
```
This will stop service and list the running services

---
## Key Learning Outcomes
- Distributed data processing with Spark
- Shuffle mechanics & execution plan analysis
- Join optimization strategies
- Data skew handling in distributed systems
- Resource tuning in YARN
- Production-grade Spark performance engineering

---
## Author
```
Parashuram V Pawar  
GitHub: Parashuram-V-Pawar
```