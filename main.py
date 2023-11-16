from flask import Flask
from flask import Flask, jsonify
import requests
import json
import sqlite3
from datetime import datetime

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
    ingestion_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_type = "weather"
    usage = "previsão do tempo"
    # values = [data['main'][city] for city in cities]

    transformed_data = {'ingestion_date': ingestion_date * len(data), 'data_type': data_type * len(data), 'values': json.dumps(values), 'usage': usage}
    return transformed_data

def load_data(transformed_data):
    conn = sqlite3.connect('weather.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather_data
                 (ingestion_date text, data_type text, values text, usage text)''')
    cursor.execute("INSERT INTO weather_data VALUES (?, ?, ?, ?)", transformed_data)
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
    # load_data(transformed_data)
    return transformed_data

if __name__ == '__main__':
    app.run(debug=True)
