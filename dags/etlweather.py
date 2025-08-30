from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.decorators import task
from datetime import datetime, timedelta



import json
import requests

LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
API_KEY = 'your_api_key'

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
}

with DAG(
    dag_id="weather_etl_pipeline",
    schedule="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    @task
    def get_weather_data():
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")
        url = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        response = http_hook.run(endpoint=url)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data. Status code: {response.status_code}")

    @task
    def transform_weather_data(weather_data):
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data

    @task
    def load_weather_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute('''CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode FLOAT,
            timestamp TIMESTAMP DEFAULT current_timestamp
        );''')

        cursor.execute('''INSERT INTO weather_data (
            latitude, longitude, temperature, windspeed, winddirection, weathercode
        ) VALUES (
            %(latitude)s, %(longitude)s, %(temperature)s, %(windspeed)s, %(winddirection)s, %(weathercode)s
        );''', transformed_data)

        conn.commit()
        cursor.close()
        conn.close()

    # ETL pipeline workflow
    weather_data = get_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)





   