import requests
import pandas as pd
import sqlite3
import mysql.connector
from mysql.connector import Error

api_key = ""
city = 'New York'
url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"

response = requests.get(url)
if response.status_code == 200:
    weather_data = response.json()  # This is a Python dictionary
    print("Data extraction successful!")
else:
    print("Error fetching data:", response.status_code)

data = {
    'city': city,
    'description': weather_data['weather'][0]['description'],
    'Temprature': weather_data['main']['temp'],
    'humidity': weather_data['main']['humidity']
}

df = pd.DataFrame([data])
print("Transformed data")
print(df)

# conn = sqlite3.connect('weather_data.db')
# cursor = conn.cursor()

# cursor.execute('''
#     CREATE TABLE IF NOT EXISTS weather (
#                id INTEGER PRIMARY KEY AUTOINCREMENT,
#                city TEXT,
#                description TEXT,
#                temprature REAL,
#                humidity INTEGER,
#                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)
# ''')

# conn.commit()

# cursor.execute('''
# INSERT INTO weather (city, description, temprature, humidity) VALUES (?,?,?,?)''',
# (data['city'], data['description'], data['Temprature'], data['humidity']))

# conn.commit()

# print("Data loaded into database successfully!")

# conn.close()


config = {
    'user' : 'root',
    'password' : 'root',
    'host' : 'localhost',
    'database' : 'weather_data'

}

try:
    connection = mysql.connector.connect(**config)

    if connection.is_connected:
        print("Connection Successfull")
        cursor = connection.cursor()

        create_table_query ="""
CREATE TABLE IF NOT EXISTS weather(
                              id integer primary key auto_increment,
                              city text,
                              description text,
                              temperature int,
                              humidity integer,
                              timestamp datetime default current_timestamp
                            )"""
        
        cursor.execute(create_table_query)
        connection.commit()
        print("Table weather is ready")

        insert_query = """INSERT INTO weather (city, description, temperature, humidity)
        VALUES (%s, %s, %s, %s)"""

        record = (data['city'], data['description'], data['Temprature'], data['humidity'])

        cursor.execute(insert_query, record)
        connection.commit()
        print("Data Inserted successfully")

except Error as e:
    print("Error",e)

finally:
    if connection.is_connected:
        cursor.close()
        connection.close()
        print("Mysql connection closed")
