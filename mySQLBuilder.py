import mysql.connector
from mysql.connector import errorcode
import glob
import json
import random

#This script builds MySQL tables that do not exist
#Checks the JSON folder for artists that don't exist in the artist table
#Then adds them into the table schema
#It updates existing entries

src= "json/*"

#-------------SCHEMA
DB_NAME = 'artist_data'
TABLES = {}

TABLES['artist_index'] = (
    "CREATE TABLE artist_index ("
    "  artist_id int NOT NULL AUTO_INCREMENT,"
    "  name varchar (25) NOT NULL,"
    "  birth smallint NOT NULL,"
    "  death smallint NOT NULL,"
    "  style tinytext NOT NULL,"
    "  bio tinytext,"
    "  wiki varchar (100),"
    "  nationality varchar (25),"
    "  PRIMARY KEY (artist_id)"
    ") ENGINE=InnoDB")

TABLES['image_index'] = (
    "CREATE TABLE image_index ("
    "  image_id INT NOT NULL,"
    "  artist_id int NOT NULL,"
    "  type varchar (25) NOT NULL,"  
    "  PRIMARY KEY (image_id)"
    ") ENGINE=InnoDB")

TABLES['image_type_details'] = (
    "CREATE TABLE image_type_details ("
    "  artist_id int NOT NULL,"
    "  type varchar (25) NOT NULL,"
    "  description tinytext"
    ") ENGINE=InnoDB")

TABLES['decades'] = (
    "CREATE TABLE decades ("
    "  artist_id int NOT NULL,"
    "  year tinyint NOT NULL"
    ") ENGINE=InnoDB")

TABLES['movements'] = (
    "CREATE TABLE movements ("
    "  artist_id int NOT NULL,"
    "  movement varchar (25) NOT NULL"
    ") ENGINE=InnoDB")

TABLES['tags'] = (
    "CREATE TABLE `tags` ("
    "  artist_id int NOT NULL,"
    "  tag varchar (25) NOT NULL"
    ") ENGINE=InnoDB")

TABLES['pitfalls'] = (
    "CREATE TABLE pitfalls ("
    "  artist_id int NOT NULL,"
    "  pitfall varchar (25) NOT NULL"
    ") ENGINE=InnoDB")


def main():
    #-----------------Try connection
    try:
        connection = mysql.connector.connect(host='localhost',
                                            port='10005',
                                            user='root',
                                            password='root')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            print("test {}".format(DB_NAME))

            #If database does not exist, create it
            cursor.execute("SHOW DATABASES LIKE '{}'".format(DB_NAME))
            if(len(cursor.fetchall()) > 0):
                print('{} already exists.'.format(DB_NAME))
            
            else:
                print('Creating ' + DB_NAME)
                cursor.execute("CREATE DATABASE {}".format(DB_NAME))

            connection.database = DB_NAME
            cursor.execute("USE {}".format(DB_NAME))
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            
            #If tables don't exist, create them
            #create_tables(cursor)

            #Load up JSONs
            loop_JSON(cursor, connection)

    except errorcode as e:
        print("Error while connecting to MySQL", e)

    #---------------disconnect    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

#-----------------------functions
def create_tables(cursor):
    #create tables if they don't exist
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

def loop_JSON(cursor, connection):
    #go through the JSON files to add
    data = []
    files = glob.glob(src, recursive=True)
    for single_file in files:
        with open(single_file, 'r') as f:
            data = json.load(f)
            print("Adding: " + data['fullname'])
            add_one_JSON(cursor, data, connection)
        
       
def add_one_JSON(cursor, data, connection):
    #add one JSON to the database
    #artist_index
    #check if artist name exists
    check_dupe=("SELECT artist_id from artist_index WHERE name=%s")
    dupe_data=(data['fullname'],)
    cursor.execute(check_dupe, dupe_data)

    if(len(cursor.fetchone())>0):
        print(dupe_data[0] + " exists. Updating.")
        add_artist=("UPDATE artist_index SET birth=%s, death=%s, style=%s, bio=%s, wiki=%s, nationality=%s"
            "WHERE name=%s")
        data_artist = (data['birthyear'], data['deathyear'], data['style'], data['biography'], data['wikilink'], data['nationality'], data['fullname'])
        cursor.execute(add_artist, data_artist)
        print(cursor.rowcount, "record(s) affected") 

    else:
        print(dupe_data + " does not exist. adding.")
        add_artist=("INSERT INTO artist_index"
            "(name, birth, death, style, bio, wiki, nationality)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s)")
        data_artist = (data['fullname'], data['birthyear'], data['deathyear'], data['style'], data['biography'], data['wikilink'], data['nationality'])
        #cursor.execute(add_artist, data_artist)
        #emp_no = cursor.lastrowid
        #print(emp_no)
        #connection.commit()
    
    #image_index
    add_image=(
        "INSERT INTO image_index"
        "(image_id, artist, type)"
        "VALUES (%i, %i, %s)"
    )
    data_artist = (XXX, data['fullname'], data['deathyear'])
    
    #image_type_details
    #decades
    #movements
    #tags
    #pitfalls

if __name__ == '__main__':
    main()