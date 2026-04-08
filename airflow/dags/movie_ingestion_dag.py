from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2
import os
 
def fetch_and_store():
    API_KEY = os.getenv("TMDB_API_KEY")
 
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
 
    cursor = conn.cursor()
 
    url = f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}"
    data = requests.get(url).json()
 
    for movie in data["results"]:
        cursor.execute("""
            INSERT INTO bronze.raw_movies (
                movie_id, title, popularity, vote_count, vote_average,
                release_date, original_language, genre_ids
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (movie_id) DO NOTHING
        """, (
            movie["id"],
            movie["title"],
            movie["popularity"],
            movie["vote_count"],
            movie["vote_average"],
            movie["release_date"],
            movie["original_language"],
            ",".join(map(str, movie["genre_ids"]))
        ))
 
    conn.commit()
    cursor.close()
    conn.close()
 
default_args = {
    'start_date': datetime(2024, 1, 1)
}
 
with DAG(
    dag_id="movie_ingestion",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
) as dag:
 
    task = PythonOperator(
        task_id="fetch_movies",
        python_callable=fetch_and_store
    )
 