import calendar
import json
import os

#c'est SDK python:  permet la communication avec AWS
import boto3
import requests
from botocore.exceptions import ClientError

BUCKET = os.environ["LAKE_BUCKET"]
#représente la base URL depuis laquelle nous allons récupérer les données du NYC Taxi & Limousine Commission
TLC = "https://d37ci6vzurychx.cloudfront.net"
# création d'objet python permettant la communication avec s3
s3 = boto3.client("s3")

# fct pour appliquer l'impodtation des données depuis le TLC vers S3

def exists(key):
  try:
    s3.head_object(Bucket=BUCKET , Key=key)
    return True
  # si l'objet n'existe pas head_object lève une erreur AWS donc on veut retourner just False
  except ClientError:
    return False
def _stream_to_s3(url,key):
  with requests.get(url, stream=True , timeout=180) as r:
    r.raise_for_status()
    r.raw.decode_content = True # veut dire prépare le contenu recu de HTTP pour le transfert
    s3.upload_fileobj(r.raw , BUCKET,key)
    
def extract_trips(year,month):
   key = f"raw/trips/year={year}/month={month:02d}/yellow_{year}-{month:02d}.parquet"
   if not exists(key):
     _stream_to_s3(f"{TLC}/trip-data/yellow_tripdata_{year}-{month:02d}.parquet" ,key)
   return key
 
def extract_zones():
   key = "raw/zones/taxi_zone_lookup.csv"
   if not exists(key):
     _stream_to_s3(f"{TLC}/misc/taxi_zone_lookup.csv" , key)
   return key
 
def extract_weather(year,month):
  key = f"raw/weather/year={year}/month={month:02d}/weather_{year}-{month:02d}.json"
  if exists(key):
    return key
  last = calendar.monthrange(year,month)[1]
  params = {
    "latitude": 40.71 , "longitude" : -74.01,
    "start_date":f"{year}-{month:02d}-01" , "end_date" : f"{year}-{month:02d}-{last}",
    "hourly": "temperature_2m,precipitation,wind_speed_10m,weather_code",
    "timezone": "America/New_York"
  }
  r = requests.get("https://archive-api.open-meteo.com/v1/archive" , params=params , timeout=60)
  r.raise_for_status()
  s3.put_object(Bucket=BUCKET , Key=key , Body=json.dumps(r.json()).encode())
  return key

