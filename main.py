from flask import Flask
from flask import Flask, jsonify
import requests
import json
import sqlite3
from datetime import datetime
import pandas as pd

app = Flask(__name__)

cities = [
    'São Paulo',
    'New York',
    'Tokyo',
    'London'
]

APIkey = 'b4085406c5ff00b581cd9fde4f9165c6'

def fetch_data():
    fetched_data = []
    for city in cities:
        url = "https://api.openweathermap.org/data/2.5/weather"
        response = requests.get(url, params={'q': city, 'units': 'metric','APIkey': APIkey})
        fetched_data.append(response.json())
    return fetched_data

def transform_data(data):
    data = [json.dumps(item) for item in data] #transfoma a lista em JSON object

    ingestion_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_type = "weather"
    usage = "previsão do tempo"

    data_dict = {
        'ingestion_date': [ingestion_date] * len(data),
        'usage': [usage] * len(data),
        'data_type': [f"{data_type} of: {city}" for city in cities],
        'data_values': data
    }
    df = pd.DataFrame(data_dict)
    return df

def load_data(df):
    conn = sqlite3.connect('weather.db')
    cursor = conn.cursor()

    df.to_sql('weather_data', conn, if_exists='append', index=False)

    conn.commit()
    conn.close()


@app.route('/')
def get():
    data = fetch_data()
    return data

@app.route('/etl')
def etl():
    data = get()
    transformed_data = transform_data(data)
    load_data(transformed_data)

    conn = sqlite3.connect('weather.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_data")
    resultado = jsonify(cursor.fetchall())
    conn.close()

    return resultado

if __name__ == '__main__':
    app.run(debug=True)
