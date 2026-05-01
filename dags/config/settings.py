from datetime import datetime, timedelta
from pathlib import Path

BASE_PATH = Path("/opt/airflow/data")
PLUGINS_PATH = Path("/opt/airflow/plugins")
SHARED_STORAGE_PATH = Path('/tmp/airflow_data')
SQL_FILE_PATH_FACT = "/opt/airflow/plugins/sql/create_tables.sql"

FILE_NAME = "movies.csv"
FULL_FILE_PATH = BASE_PATH / FILE_NAME


AIRFLOW_CONFIG = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False,
}
POSTGRES_CONN_ID = 'postgres_local'
MONGO_CONN_STRING = "mongodb+srv://macherrab:cherrab123@movie.qcrwhyy.mongodb.net/movie"

FILE_SENSOR_CONFIG = {
    'poke_interval': 8,
    'timeout': 100,
    'mode': "reschedule"
}

