import mysql.connector
from mysql.connector import Error
import json, requests, sys
import datetime
import time

try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        db='meteorologica'
    )
    if conn.is_connected():
        cursor = conn.cursor()

except Error as ex:
    print(ex)
finally:
    if conn.is_connected():
        print('Conexión exitosa')


# Download the JSON data from OpenWeatherMap.org's API
def peticionApi():
    url = 'http://api.openweathermap.org/data/2.5/weather?q=Quito&appid=f3147d5a1dd66ec0cf337171249178fc'
    resp = requests.get(url)
    global clima
    clima = resp.json()

    lat = clima['coord']['lat']
    lon = clima['coord']['lon']

    url = 'https://api.openweathermap.org/data/2.5/onecall?lat=-0.2299&lon=-78.5249&exclude=hourly,daily&appid=f3147d5a1dd66ec0cf337171249178fc'
    resp = requests.get(url)
    global clima2
    clima2 = resp.json()

    print(clima)
    print(clima2)

#print(clima['main']['humidity']) #temp, pressure, humidity
#print(clima['wind']) #speed, deg

#guardar solo temperatura


def temp():
    now = datetime.datetime.now();
    date = now.strftime('%Y-%m-%d %H:%M:%S')
    temp = clima['main']['temp'] - 273.15;
    sql = "INSERT INTO temperatura(temp, dat_hour) VALUES (%s,'%s')" % (temp, date)
    cursor.execute(sql)
    conn.commit()

def wind():
    now = datetime.datetime.now();
    date = now.strftime('%Y-%m-%d %H:%M:%S')
    vel = clima['wind']['speed']
    dir = clima['wind']['deg']
    sql = "INSERT INTO viento(viento_vel, viento_dire, viento_fecha) VALUES (%s,%s,'%s')" % (vel, dir, date)
    cursor.execute(sql)
    conn.commit()

def pressure():
    now = datetime.datetime.now();
    date = now.strftime('%Y-%m-%d %H:%M:%S')
    pres_atm = clima['main']['pressure']
    sql = "INSERT INTO presion(presion_atm, presion_fecha) VALUES (%s,'%s')" % (pres_atm, date)
    cursor.execute(sql)
    conn.commit()

def humidity():
    now = datetime.datetime.now();
    date = now.strftime('%Y-%m-%d %H:%M:%S')
    humidity = clima['main']['humidity']
    sql = "INSERT INTO humedad(lev_hum, dat_hou) VALUES (%s,'%s')" % (humidity, date)
    cursor.execute(sql)
    conn.commit()

def rain():
    now = datetime.datetime.now();
    date = now.strftime('%Y-%m-%d %H:%M:%S')
    if(clima['weather'][0]['main']!='Rain'):
        rain = 0
    else:
        rain = clima['rain']['1h']

    sql = "INSERT INTO precipitacion(presi_nivel, presi_fecha) VALUES (%s,'%s')" % (rain, date)
    cursor.execute(sql)
    conn.commit()

def uv():
    now = datetime.datetime.now();
    date = now.strftime('%Y-%m-%d %H:%M:%S')
    level_radsol = clima2['current']['uvi']
    sql = "INSERT INTO radiacion(rad_nivel, rad_fecha) VALUES (%s,'%s')" % (level_radsol, date)
    cursor.execute(sql)
    conn.commit()


def evapotranspiration():

    sql = "SELECT * FROM radiacion ORDER BY id DESC LIMIT 1";
    cursor.execute(sql)
    radiacion = cursor.fetchall()

    sql2 = "SELECT * FROM temperatura ORDER BY id DESC LIMIT 1";
    cursor.execute(sql2)
    temperatura = cursor.fetchall()

    fecha = temperatura[0][2]
    evapotranspiracion = 0.0135 * (temperatura[0][1] + 17.78) * radiacion[0][1]

    sql3 = "INSERT INTO transpiracion (trans, dat_hour) VALUES (%.2f,'%s')" % (evapotranspiracion, fecha)
    cursor.execute(sql3)
    conn.commit()

def dew():
    now = datetime.datetime.now();
    date = now.strftime('%Y-%m-%d %H:%M:%S')
    dew = clima2['current']['dew_point'] - 273.15
    sql = "INSERT INTO rocio(rocio_nivel, rocio_fecha) VALUES (%s,'%s')" % (dew, date)
    cursor.execute(sql)
    conn.commit()


def selectEvapo():
    sql = "SELECT id FROM transpiracion ORDER BY id DESC LIMIT 1 "
    cursor.execute(sql)
    id = cursor.fetchone()
    for i in id:
        return i

#Ejecutar el simulador para obtener datos cada 1h
'''
while(True):
    peticionApi()
    temp()
    wind()
    humidity()
    uv()
    dew()
    rain()
    pressure()
    evapotranspiration()
    time.sleep(3600)
'''

#duplicacion de datos reales

def selectTemperatura():
    sql = "SELECT * FROM temperatura"
    cursor.execute(sql)
    id = cursor.fetchall()
    fecha = id[-1][2] #Recupera la fecha más actual en la base de datos,
    # esto sirve para que al duplicar datos tome la última fecha y le vaya sumando en cada iteracion 1 hora

    for i in id:
        fecha = fecha + datetime.timedelta(hours=1);
        sql = "INSERT INTO temperatura (temp, dat_hour) values(%s, '%s')"% (i[1], fecha)
        cursor.execute(sql)
        conn.commit()
        #print("%s, %s, %s"% (i[0], i[1], fecha)) #Esto lo descomentan, y comentan el resto por si gustan saber como aumenta 1 hora las fechas para el nuevo ingreso de datos

def selectHumedad():
    sql = "SELECT * FROM humedad"
    cursor.execute(sql)
    id = cursor.fetchall()
    fecha = id[-1][2]

    for i in id:
        fecha = fecha + datetime.timedelta(hours=1);
        sql = "INSERT INTO humedad (lev_hum, dat_hour) values(%s, '%s')"% (i[1], fecha)
        cursor.execute(sql)
        conn.commit()

def selectPrecipitacion():
    sql = "SELECT * FROM precipitacion"
    cursor.execute(sql)
    id = cursor.fetchall()
    fecha = id[-1][2]

    for i in id:
        fecha = fecha + datetime.timedelta(hours=1);
        sql = "INSERT INTO precipitacion (presi_nivel, dat_hour) values(%s, '%s')"% (i[1], fecha)
        cursor.execute(sql)
        conn.commit()

def selectPresion():
    sql = "SELECT * FROM presion"
    cursor.execute(sql)
    id = cursor.fetchall()
    fecha = id[-1][2]

    for i in id:
        fecha = fecha + datetime.timedelta(hours=1);
        sql = "INSERT INTO presion (presion_atm, dat_hour) values(%s, '%s')"% (i[1], fecha)
        cursor.execute(sql)
        conn.commit()

def selectRadiacion():
    sql = "SELECT * FROM radiacion"
    cursor.execute(sql)
    id = cursor.fetchall()
    fecha = id[-1][2]

    for i in id:
        fecha = fecha + datetime.timedelta(hours=1);
        sql = "INSERT INTO radiacion (rad_nivel, dat_hour) values(%s, '%s')"% (i[1], fecha)
        cursor.execute(sql)
        conn.commit()

def selectRocio():
    sql = "SELECT * FROM rocio"
    cursor.execute(sql)
    id = cursor.fetchall()
    fecha = id[-1][2]

    for i in id:
        fecha = fecha + datetime.timedelta(hours=1);
        sql = "INSERT INTO rocio (rocio_nivel, dat_hour) values(%s, '%s')"% (i[1], fecha)
        cursor.execute(sql)
        conn.commit()

def selectTranspiracion():
    sql = "SELECT * FROM transpiracion"
    cursor.execute(sql)
    id = cursor.fetchall()
    fecha = id[-1][2]

    for i in id:
        fecha = fecha + datetime.timedelta(hours=1);
        sql = "INSERT INTO transpiracion (trans, dat_hour) values(%s, '%s')"% (i[1], fecha)
        cursor.execute(sql)
        conn.commit()

def selectViento():
    sql = "SELECT * FROM viento"
    cursor.execute(sql)
    id = cursor.fetchall()
    fecha = id[-1][3]

    for i in id:
        fecha = fecha + datetime.timedelta(hours=1);
        sql = "INSERT INTO viento (viento_vel, viento_dire, dat_hour) values(%s, %s, '%s')"% (i[1], i[2], fecha)
        cursor.execute(sql)
        conn.commit()

#Descomentar para la duplicación de datos dependiendo de la necesidad:
'''
selectViento()
selectTemperatura()
selectHumedad()
selectPrecipitacion()
selectPresion()
selectRadiacion()
selectRocio()
selectTranspiracion()
'''