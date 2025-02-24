import mysql.connector.cursor
import requests
import mysql.connector
from mysql.connector import Error


api_key = ""
lat = int(input("Enter latitude: "))
lon = int(input("Enter longitude: "))
url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"

config = {
    'user' : 'root',
    'password' : 'root',
    'host' : 'localhost',
    'database' : 'air_quality'
}

response = requests.get(url)
df = response.json()
print(df)

data = {
    'aqi' : df['list'][0]['main']['aqi'],
    'co' : df['list'][0]['components']['co'],
    'no' : df['list'][0]['components']['no'],
    'no2' : df['list'][0]['components']['no2'],
    'o3' : df['list'][0]['components']['o3'],
    'so2' : df['list'][0]['components']['so2'],
    'pm2.5' : df['list'][0]['components']['pm2_5'],
    'pm10' : df['list'][0]['components']['pm10'],
    'nh3' : df['list'][0]['components']['nh3'],

}
print(data)
try:
    connection = mysql.connector.connect(**config)
    if connection.is_connected:
        print("Mysql Connection Successfull")
        cursor = connection.cursor()
        create_table_query = """
Create table if not exists airquality(
        id int primary key auto_increment,
        aqi float,
        co float,
        no float,
        no2 float,
        o3 float,
        so2 float,
        pm2_5 float,
        pm10 float,
        nh3 float,
        timestamp timestamp default current_timestamp
        )"""

        cursor.execute(create_table_query)
        connection.commit()
        print("Table Created successfully")

        insert_query = """insert into airquality(aqi,co,no,no2,o3,so2,pm2_5,pm10,nh3)
        value(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        record = (data['aqi'],data['co'],data['no'],data['no2'],data['o3'],data['so2'],
                  data['pm2.5'],data['pm10'],data['nh3'])
        
        cursor.execute(insert_query,record)
        connection.commit()
        print("Data Inserted successfully")


except Error as e:
    print("Error",e)

finally:
    if connection.is_connected:
        cursor.close()
        connection.close()
        print("Mysql Connection closed")



hl
