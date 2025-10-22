# Premier League Injury Impact Analysis - ETL Pipeline
This project is an end-to-end data engineering solution designed to analyze the impact of player injuries on team performance in the English Premier League. By combining injury data from Premierinjuries.com with advanced performance statistics from Understat.com, the system provides quantitative insights into how personnel changes affect match outcomes and team dynamics.
An article explaining the process of building this project is available [here]()

## Project Structure

```plaintext
InjuryAnalysisETL/  # Root directory
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
- Python 3.8+ installed
- PostgreSQL 13+ database server
- Apache Airflow 2.0+

The project also requires the following Python Packages:
- BeautifulSoup4
- Requests
- Psycopg2-binary
- Pandas
- lxml parser


## Project Setup
### 1. Clone the Repository
```
git clone https://github.com/yourusername/prem-injury-analysis.git
cd prem-injury-analysis
```
### 2. Create  and install Virtual Environment
```python
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### 3. Initialize Apache Airflow

> NOTE: For Airflow, run airflow db init to initialize a new instance of airflow.cfg, Airflow's config file and airflow.db a SQLite database instance. This project doesn't have these files as they have private information.

#### a. Set AIRFLOW_HOME.
Run the following command to set AIRFLOW_HOME where our Airflow configuration will reside at
```
# Set Airflow home directory
export AIRFLOW_HOME=~/airflow
```

Initialize Airflow config files
Run the following command to initialize the files into the airflow folder
```
# Initialize the database
airflow db init
```
#### b.Create admin user
Run the following command to create an Admin user to access the Airflow UI.
```
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```
#### c. Make the following changes to the settings in the configuration file
Access the file by running
```
nano airflow/airflow.cfg
```
Make changes to the following settings:
```
load_settings=False # default True

[database]
sqlalchemy_conn_url = POSTGRES_URL # default SQLite Conn string 

```
Save the changes made then migrate changes to the newly connected PostgreSQL database

#### d. Migrate changes to the database
```
airflow db migrate
```

#### e. Run the webserver and scheduler
```

# Start the web server (in one terminal)
airflow webserver --port 8080

# Start the scheduler (in another terminal)
airflow scheduler
```

The Airflow UI will start on port 8080, go to localhost:8080 to access.





