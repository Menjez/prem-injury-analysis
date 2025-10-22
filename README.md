# Premier League Injury Impact Analysis - ETL Pipeline
This project is an end-to-end data engineering solution designed to analyze the impact of player injuries on team performance in the English Premier League. By combining injury data from Premierinjuries.com with advanced performance statistics from Understat.com, the system provides quantitative insights into how personnel changes affect match outcomes and team dynamics.
An article explaining the process of building this project is available [here]()

## Project Structure

```plaintext
FoodPricesMonitoring/  # Root directory
│
├── airflow/                
│   ├── dags/
│   │   └── injury_dag.py
│   │
│   ├── injuryetl/
│   │   ├── extract.py
│   │   ├── transform.py
│   │   └── load.py
│   │
│   ├── airflow.cfg
│   └── airflow.db
│
└── README.md
```

## Database Schema Diagram
<img width="518" height="715" alt="injury schema" src="https://github.com/user-attachments/assets/8a33f6b3-8c5e-4bea-a87a-c857c04d4ffe" />

## Pre Requisites
Before installing this project, ensure you have the following:
Python 3.8+ installed
PostgreSQL 13+ database server
Apache Airflow 2.0+

The project also requires the following Python Packages:
BeautifulSoup4
Requests
Psycopg2-binary
Pandas
lxml parser


## Project Setup
1. Clone the Repository
```
git clone https://github.com/yourusername/premier-league-etl.git
cd premier-league-etl
```
2. Create  and install Virtual Environment
```
bash# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

Initialize Apache Airflow
```
# Set Airflow home directory
export AIRFLOW_HOME=~/airflow

# Initialize the database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Set settings in airflow.cfg file
load_settings=False # default True

[database]
sqlalchemy_conn_url = POSTGRES_URL # default SQLite Conn string 


# Copy DAG files to Airflow directory
cp dags/premier_league_etl_dag.py $AIRFLOW_HOME/dags/

# Start the web server (in one terminal)
airflow webserver --port 8080

# Start the scheduler (in another terminal)
airflow scheduler
```

The Airflow UI will start on port 8080, go to localhost:8080 to access.





