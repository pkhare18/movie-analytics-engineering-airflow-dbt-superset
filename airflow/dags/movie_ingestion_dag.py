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
 
    endpoints = ["popular", "top_rated", "upcoming"]
 
    for endpoint in endpoints:
        for page in range(1, 11):  # 🔥 pagination
 
            url = f"https://api.themoviedb.org/3/movie/{endpoint}?api_key={API_KEY}&page={page}"
 
            response = requests.get(url)
            data = response.json()
 
            for movie in data.get("results", []):
 
                cursor.execute("""
                    INSERT INTO bronze.raw_movies (
                        movie_id,
                        title,
                        popularity,
                        vote_count,
                        vote_average,
                        release_date,
                        original_language,
                        genre_ids,
                        source_type
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (movie_id) DO NOTHING
                """, (
                    movie.get("id"),
                    movie.get("title"),
                    movie.get("popularity"),
                    movie.get("vote_count"),
                    movie.get("vote_average"),
                    movie.get("release_date"),
                    movie.get("original_language"),
                    str(movie.get("genre_ids")),
                    endpoint   # 🔥 source tracking
                ))
 
    conn.commit()
    cursor.close()
    conn.close()
 
 
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}
 
 
with DAG(
    dag_id="movie_ingestion",
    default_args=default_args,
    schedule_interval=None,  # manual for now
    catchup=False
) as dag:
 
    fetch_movies = PythonOperator(
        task_id="fetch_movies",
        python_callable=fetch_and_store
    )
 
    fetch_movies