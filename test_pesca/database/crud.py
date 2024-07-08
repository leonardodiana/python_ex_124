from database.db import *

#funzione per collegarmi al db
def create_connection():
    connection = sqlite3.connect('database/db.sqlite')
    return connection


def read_importanza_by_anno(anno_da:int, anno_a:int):
    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM importanza WHERE Anno BETWEEN ? and ?", (anno_da, anno_a,))
    result = [dict((cursor.description[i][0], value) \
               for i, value in enumerate(row)) for row in cursor.fetchall()]
    connection.commit()
    connection.close()
    return result

def read_produttività_by_anno(anno_da:int, anno_a:int):
    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM produttivita WHERE Anno BETWEEN ? and ?", (anno_da, anno_a,))
    result = [dict((cursor.description[i][0], value) \
               for i, value in enumerate(row)) for row in cursor.fetchall()]
    connection.commit()
    connection.close()
    return result

def read_andamento_by_anno(anno_da:int, anno_a:int):
    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM andamento WHERE Anno BETWEEN ? and ?", (anno_da, anno_a,))
    result = [dict((cursor.description[i][0], value) \
               for i, value in enumerate(row)) for row in cursor.fetchall()]
    connection.commit()
    connection.close()
    return result

def read_serie_calcolate_by_anno(anno_da:int, anno_a:int):
    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM serie_calcolate WHERE Anno BETWEEN ? and ?", (anno_da, anno_a,))
    result = [dict((cursor.description[i][0], value) \
               for i, value in enumerate(row)) for row in cursor.fetchall()]
    connection.commit()
    connection.close()
    return result