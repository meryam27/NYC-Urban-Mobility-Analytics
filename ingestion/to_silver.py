import io
import json

import os
import boto3
import pandas as pd

BUCKET = os.environ["LAKE_BUCKET"]
s3 = boto3.client('s3')

RENAME = {
  "VendorID": "vendor_id", 
  "tpep_pickup_datetime": "pickup_at",
  "tpep_dropoff_datetime": "dropoff_at",
  "PULocationID": "pickup_location_id",
  "DOLocationID": "dropoff_location_id",
  
}

KEEP = ["vendor_id", 
        "pickup_at", 
        "dropoff_at", 
        "passenger_count", 
        "trip_distance", # distance en milles
        "pickup_location_id",  # (dans fichier de projet "pu_location_id"  )id ce trouve dans csv taxi_zone_lookup.csv
        "dropoff_location_id", # (dans fichier de projet "do_location_id"  ) de meme
        "payment_type", # card , cash ..
        "fare_amount" , # prix calculé à partire de distance * prix pour chaque mille
        "tip_amount",  # pourboir
        "tolls_amount" ,
        "total_amount"]

# lecture 
def _read(key):
  return s3.get_object(Bucket=BUCKET , Key=key)["Body"].read()

# l'écriture
def _write(df,key):
  # on crée un buffer en mémoire
  buf = io.BytesIO()
  # index c'est la colonne que pd peut ajouter 
  # transformer dataframe en parquet pour l'envoyer a s3
  df.to_parquet(buf, index = False, coerce_timestamps='ms',allow_truncated_timestamps=True ) # en ms car pandas travail avec nano s une valeur que athena peut lire de manière fausse
  # charger d'apres buffer
  s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
  
def clean_trips(df, year, month):
   
    df = df.rename(columns=RENAME)[KEEP]
    for c in ["vendor_id", "passenger_count", "pickup_location_id", "dropoff_location_id", "payment_type"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int32")
    for c in ["trip_distance", "fare_amount", "tip_amount", "tolls_amount", "total_amount"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    df["pickup_at"] = pd.to_datetime(df["pickup_at"])
    df["dropoff_at"] = pd.to_datetime(df["dropoff_at"])
    return df[(df.pickup_at.dt.year == year) & (df.pickup_at.dt.month == month)]

def trips_to_silver(year, month):
    df = pd.read_parquet(io.BytesIO(_read(
        f"raw/trips/year={year}/month={month:02d}/yellow_{year}-{month:02d}.parquet")))
    _write(clean_trips(df, year, month), f"clean/trips/year={year}/month={month:02d}/trips.parquet")
    

  
def weather_to_silver(year,month):
  h = json.loads(_read(f"raw/weather/year={year}/month={month:02d}/weather_{year}-{month:02d}.json"))
  hourly = h["hourly"]
  df = pd.DataFrame({
    "observed_at": pd.to_datetime(hourly["time"]),
    "temperature_c": hourly["temperature_2m"],
    "precipitation_mm": hourly["precipitation"],
    "wind_speed_kmh": hourly["wind_speed_10m"],
    "weather_code": hourly["weather_code"],})
  df["weather_code"] = df["weather_code"].fillna(0).astype("int32")
  _write(df,f"clean/weather/year={year}/month={month:02d}/weather.parquet")
  
def zones_to_silver():
    z = pd.read_csv(io.BytesIO(_read("raw/zones/taxi_zone_lookup.csv")))
    z.columns = ["location_id", "borough", "zone", "service_zone"] # avant : LocationID , Borough , Zone ,service_zone
    _write(z, "clean/zones/zones.parquet")
