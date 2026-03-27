import requests
import os
from dotenv import load_dotenv
import json

load_dotenv()

API_KEY=os.getenv("TMDB_API_KEY")

url=f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}"

print(url)

response=requests.get(url)

print("Status: ",response.status_code)

data=response.json()

#print(data)


print(data["results"][0])

print(json.dumps(data["results"][0],indent=4))