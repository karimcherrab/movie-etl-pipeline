import os
import sys
import pandas as pd
from airflow import DAG

from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from helpers.transformations import MovieDataTransformer
from helpers.Loading import MovieDataLoading

# Settings
from config.settings import AIRFLOW_CONFIG
from config.settings import PLUGINS_PATH
from config.settings import SHARED_STORAGE_PATH
from config.settings import FILE_NAME
from config.settings import FULL_FILE_PATH
from config.settings import SQL_FILE_PATH_FACT

sys.path.append(PLUGINS_PATH)


os.makedirs(SHARED_STORAGE_PATH, exist_ok=True)


def read_file_func():
    try:
        print(f"Tentative de lecture du fichier: {FULL_FILE_PATH}")
        if os.path.exists(FULL_FILE_PATH):
            with open(FULL_FILE_PATH, "r") as f:
                content = f.read()
                print(f"Taille du fichier: {len(content)} caractères")
                return content
        else:
            print(f"Le fichier {FULL_FILE_PATH} n'existe pas!")
    except Exception as e:
        print(f"Erreur lors de la lecture: {str(e)}")


def data_Transformer(ti):
    try:
        if not os.path.exists(FULL_FILE_PATH):
            raise FileNotFoundError(f"{FULL_FILE_PATH} introuvable")
        df = pd.read_csv(FULL_FILE_PATH)

        movieDataTransformer = MovieDataTransformer(ti)
        movieDataTransformer.clean_movie(df)


    except Exception as e:
        print(f"Erreur lors de la lecture: {str(e)}")


def load_dim_movie(ti):
    file_path = ti.xcom_pull(
        task_ids="nettoyage_data",
        key='movies_path'
    )

    genres = pd.read_parquet(file_path)
    load = MovieDataLoading()
    load.insert_dim_movie(genres)


def load_dim_genre(ti):
    file_path = ti.xcom_pull(
        task_ids="nettoyage_data",
        key='genres_path'
    )

    genres = pd.read_parquet(file_path)
    load = MovieDataLoading()
    load.insert_dim_genre(genres)


def load_dim_date(ti):
    file_path = ti.xcom_pull(
        task_ids="nettoyage_data",
        key='movies_path'
    )

    data = pd.read_parquet(file_path)
    load = MovieDataLoading()
    load.insert_dim_date(data)


def load_dim_actor(ti):
    file_path = ti.xcom_pull(
        task_ids="nettoyage_data",
        key='cast_path'
    )

    data = pd.read_parquet(file_path)
    load = MovieDataLoading()
    load.insert_dim_actor(data)


def load_bridge_movie_genre(ti):
    file_path_genre = ti.xcom_pull(
        task_ids="nettoyage_data",
        key='genres_path'
    )
    file_path_movie = ti.xcom_pull(
        task_ids="nettoyage_data",
        key='movies_path'
    )

    data_movie = pd.read_parquet(file_path_movie)
    data_genre = pd.read_parquet(file_path_genre)
    load = MovieDataLoading()
    load.insert_bridge_movie_genre(data_movie, data_genre)


def load_bridge_movie_actor(ti):
    file_path_genre = ti.xcom_pull(
        task_ids="nettoyage_data",
        key='cast_path'
    )
    file_path_movie = ti.xcom_pull(
        task_ids="nettoyage_data",
        key='movies_path'
    )

    data_movie = pd.read_parquet(file_path_movie)
    data_cast = pd.read_parquet(file_path_genre)
    load = MovieDataLoading()
    load.insert_bridge_movie_actor(data_movie, data_cast)


def load_fact_movie(ti):
    file_path = ti.xcom_pull(
        task_ids="nettoyage_data",
        key='movies_path'
    )

    movie = pd.read_parquet(file_path)
    load = MovieDataLoading()
    load.insert_fact_movie(movie)


def load_review(ti):
    file_path = ti.xcom_pull(
        task_ids="nettoyage_data",
        key='reviews_path'
    )

    movie = pd.read_parquet(file_path)
    load = MovieDataLoading()
    load.insert_review(movie)




dag_movie = DAG(
    dag_id="movie_data_pipeline3",
    description="Pipeline ETL pour les films",
    default_args=AIRFLOW_CONFIG,
    schedule=None,
    catchup=False,
)

wait_file = FileSensor(
    task_id="wait_file_movies",
    filepath=FILE_NAME,
    poke_interval=8,
    timeout=100,
    mode="reschedule",
    fs_conn_id="local_file",
    dag=dag_movie
)

nettoyageData = PythonOperator(
    task_id="nettoyage_data",
    python_callable=data_Transformer,
    dag=dag_movie

)


create_fact_tables = SQLExecuteQueryOperator(
    task_id="create_fact_tables",
    conn_id="postgres_local",
    sql=open(SQL_FILE_PATH_FACT).read(),
    dag=dag_movie
)

load_dim_movie_tache = PythonOperator(
    task_id="load_dim_movie",
    python_callable=load_dim_movie,
    dag=dag_movie
)

load_dim_genre_tache = PythonOperator(
    task_id="load_dim_genre",
    python_callable=load_dim_genre,
    dag=dag_movie
)

load_dim_date_tache = PythonOperator(
    task_id="load_dim_date",
    python_callable=load_dim_date,
    dag=dag_movie

)

load_dim_actor_tache = PythonOperator(
    task_id="load_dim_actor",
    python_callable=load_dim_actor,
    dag=dag_movie

)
load_bridge_movie_genre_tache = PythonOperator(
    task_id="load_bridge_movie_genre",
    python_callable=load_bridge_movie_genre,
    dag=dag_movie

)
load_bridge_movie_actor_tache = PythonOperator(
    task_id="load_bridge_movie_actor",
    python_callable=load_bridge_movie_actor,
    dag=dag_movie

)
load_fact_movie_tache = PythonOperator(
    task_id="load_fact_movie",
    python_callable=load_fact_movie,
    dag=dag_movie

)
load_review = PythonOperator(
    task_id="load_review",
    python_callable=load_review,
    dag=dag_movie
)

wait_file >> nettoyageData
nettoyageData >>  [load_review, create_fact_tables]
create_fact_tables >> [load_dim_movie_tache,load_dim_genre_tache] >> load_bridge_movie_genre_tache >> load_dim_actor_tache >> load_bridge_movie_actor_tache >> load_dim_date_tache >> load_fact_movie_tache


