import requests
import psycopg2
import os
from dotenv import load_dotenv
 
load_dotenv()
 
API_KEY = os.getenv("TMDB_API_KEY")
 
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}
 
def fetch_movies():
    all_movies=[]

    for page in range(1,6):
        url = f"https://api.themoviedb.org/3/movie/popular?page={page}&api_key={API_KEY}"
        response = requests.get(url)
 
        if response.status_code != 200:
            raise Exception(f"API Error: {response.status_code}")
 
        data = response.json()
        all_movies.extend(data["results"])

    
    return all_movies
 
def insert_movies(movies):
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
 
    print("Total fetched:", len(movies))
    print("Unique IDs:", len(set([m["id"] for m in movies])))

    for movie in movies:
        try:
            cursor.execute("""
                INSERT INTO raw_movies (
                    movie_id, title, popularity, vote_count, vote_average,
                    release_date, original_language, genre_ids
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (movie_id) DO NOTHING                
            """, (
                movie.get("id"),
                movie.get("title"),
                movie.get("popularity"),
                movie.get("vote_count"),
                movie.get("vote_average"),
                movie.get("release_date") or None,
                movie.get("original_language"),
                ",".join(map(str, movie.get("genre_ids", [])))
            ))
        except Exception as e:
            print("Error inserting movie:", e)
            conn.rollback()
 
    conn.commit()
    cursor.close()
    conn.close()
 
def main():
    print("Fetching data from TMDb...")
    movies = fetch_movies()
 
    print(f"Fetched {len(movies)} records")
 
    print("Inserting into Postgres...")
    insert_movies(movies)
 
    print("Ingestion completed successfully!")
 
if __name__ == "__main__":
    main()