from time import time
import pymysql
import os
import re

from limpieza import main as filtrar_datos

DATA_PATH = ''


def crear_conexion(
                    user: str, password: str,
                    db='', host='localhost'
                    ) -> pymysql.connections.Connection:
    '''
    Creará una conexion a una base de datos

    Args:
        user: Usuario de MySQL
        password: la clave del usuario
        db: si se especifica, se conectará directamente a esa base de datos
        host: localhost u otros

    Returns: la conexion de la libreria pymysql
    '''

    if db:
        conexion = pymysql.connect(
                    host=host,
                    user=user,
                    password=password,
                    database=db
                    )
    else:
        conexion = pymysql.connect(
                    host=host,
                    user=user,
                    password=password,
                    )

    return conexion


def crear_db(
                user: str, password: str,
                nombre: str) -> pymysql.connections.Connection:
    '''
    Crea una base de datos dentro de nuestra conexión

    Args:
        conexion: servidor donde se quiere crear la bd
        nombre: el nombre que va a llevar la bd

    Returns:
        Nueva conexion, ya dentro de la nueva bd
    '''

    conexion = crear_conexion(user, password)
    with conexion:
        cursor = conexion.cursor()
        sql = "CREATE DATABASE IF NOT EXISTS " + str(nombre)
        cursor.execute(sql)
        cursor.close()

    return crear_conexion(user, password, db=nombre)


def crear_tablas(conexion: pymysql.connections.Connection) -> None:
    '''
    Creará las relaciones segun las descritas en el modelo relacional

    Args:
        conexion: conexion a la db donde crear las tablas

    Returns: None
    '''
    cursor = conexion.cursor()
    sql_drop = "DROP TABLE IF EXISTS %s"
    tablas = ['escuchas', 'canciones', 'artistas', 'usuarios']

    # Borramos las posibles tablas
    for tabla in tablas:
        cursor.execute(sql_drop % tabla)

    # Y vamos creando todas las tablas necesarias
    sql_usuarios = '''CREATE TABLE usuarios (
                        ID int(4) NOT NULL,
                        GENERO varchar(1),
                        EDAD int(3),
                        PAIS varchar(40),
                        FECHA_REGISTRO date,
                        PRIMARY KEY (ID))'''
    cursor.execute(sql_usuarios)

    sql_canciones = '''CREATE TABLE canciones (
                        ID int(6) NOT NULL,
                        TRAID varchar(36),
                        TRANAME varchar(140),
                        PRIMARY KEY (ID))'''
    cursor.execute(sql_canciones)

    sql_artistas = '''
        CREATE TABLE artistas (
            ID int(6) NOT NULL,
            ARTID varchar(36),
            ARTNAME varchar(100),
            PRIMARY KEY (ID)
        )
        '''
    cursor.execute(sql_artistas)

    sql_escuchas = '''CREATE TABLE escuchas(
                        ID int(7) NOT NULL,
                        USERID int(4),
                        ARTID int(6),
                        TRAID int(6),
                        FECHA datetime,
                        PRIMARY KEY (ID),
                        CONSTRAINT escuchas_ibfk_1 FOREIGN KEY (USERID)
                        REFERENCES usuarios (ID) ON DELETE CASCADE,

                        CONSTRAINT escuchas_ibfk_2 FOREIGN KEY (ARTID)
                        REFERENCES artistas (ID) ON DELETE CASCADE,

                        CONSTRAINT escuchas_ibfk_3 FOREIGN KEY (TRAID)
                        REFERENCES canciones (ID) ON DELETE CASCADE)'''
    cursor.execute(sql_escuchas)
    cursor.close()

    return


def insertar_datos(conexion: pymysql.connections.Connection) -> None:
    '''
    Se insertarán los datos recopilados en los tsv.
    Comprobaremos si existen o no los archivos necesarios para insertar los
    datos. Sino, se filtrarán los datos y se guardarán para posibles futuros

    Args:
        conexion: la conexion desde la que se harán las consultas sql

    Returns: None
    '''

    def separar_tabs(tsv: list) -> list:
        '''
        Se encarga de separar cada elemento de un tsv en
        un elemento de una tupla

        Args:
            tsv: lista en la que cada elemento está separado por \t

        Returns:
            lista de tuplas, en la que cada valor es un elemento del tsv
        '''

        values = []
        for linea in tsv:
            tmp = linea.split('\t')
            # Descartaremos las lineas completamente vacías
            if tmp != ['\n']:
                for i in range(len(tmp)):
                    # Eliminaremos los saltos de línea
                    tmp[i] = re.sub('\n', '', tmp[i])
                    # Detectamos los valores vacíos
                    if not tmp[i]:
                        tmp[i] = None
                values.append(tuple(tmp))

        return values

    archivos_necesarios = [
                        'artistas_filtrado.tsv', 'canciones_filtrado.tsv',
                        'escuchas_filtrado.tsv', 'usuarios_filtrado.tsv'
                        ]

    # Veamos si existen los archivos
    limpieza_necesaria = False
    for archivo in archivos_necesarios:
        if not os.path.isfile(f'./data/{archivo}'):
            limpieza_necesaria = True
            break

    # Si es necesario, crearemos estos archivos
    if limpieza_necesaria:
        escuchas, canciones, artistas, usuarios = filtrar_datos(DATA_PATH)
    else:
        with open('./data/escuchas_filtrado.tsv', 'r', encoding='utf-8') as f:
            escuchas = f.readlines()

        with open('./data/artistas_filtrado.tsv', 'r', encoding='utf-8') as f:
            artistas = f.readlines()

        with open('./data/canciones_filtrado.tsv', 'r', encoding='utf-8') as f:
            canciones = f.readlines()

        with open('./data/usuarios_filtrado.tsv', 'r', encoding='utf-8') as f:
            usuarios = f.readlines()

    print('Datos filtrados correctamente')

    # Pasemoslo ahora al formato que nos interesa a la hora de insertar datos
    usuarios = separar_tabs(usuarios[1:])
    escuchas = separar_tabs(escuchas[1:])
    artistas = separar_tabs(artistas[1:])
    canciones = separar_tabs(canciones[1:])

    cursor = conexion.cursor()
    # Dejaremos las escuchas para el final
    sql_usuarios = """INSERT INTO usuarios
                    (ID, GENERO, EDAD, PAIS, FECHA_REGISTRO)
                    VALUES(%s, %s, %s, %s, %s)"""
    cursor.executemany(sql_usuarios, usuarios)
    conexion.commit()

    sql_artistas = """INSERT INTO artistas
                    (ID, ARTID, ARTNAME)
                    VALUES(%s, %s, %s)"""
    cursor.executemany(sql_artistas, artistas)
    conexion.commit()

    sql_canciones = """INSERT INTO canciones
                    (ID, TRAID, TRANAME)
                    VALUES(%s, %s, %s)"""
    cursor.executemany(sql_canciones, canciones)
    conexion.commit()

    sql_escuchas = """INSERT INTO escuchas
                    (ID, USERID, ARTID, TRAID, FECHA)
                    VALUES(%s, %s, %s, %s, %s)"""
    cursor.executemany(sql_escuchas, escuchas)
    conexion.commit()

    cursor.close()


def main() -> pymysql.connections.Connection:
    '''
    Ejecuta el programa, en el cual se seguirán los siguientes pasos:
        - Filtrar los datos para recuperar los valores que nos interesan
        - Crear una base de datos segun el esquema relacional adjunto
        - Insertar los datos filtrados en la bd
        - Realizar una serie de consultas

    Args:
        user: usuario de MySQL
        password: clave del usuario para acceder al servidor

    Returns:
        conexion con toda la bd creada
    '''
    global DATA_PATH

    nombre_db = "lastfm"
    with open('./config.txt', 'r') as archivo:
        user = re.sub('Usuario:', '', archivo.readline()).strip()
        password = re.sub('Clave:', '', archivo.readline()).strip()
        DATA_PATH = re.sub('PATH:', '', archivo.readline()).strip()

    conn = crear_db(user, password, nombre_db)
    print('Base de datos creada correctamente')
    crear_tablas(conn)
    print('Tablas creadas correctamente')
    insertar_datos(conn)
    print('Todos los datos han sido insertados correctamente')

    return conn


if __name__ == '__main__':

    main()
