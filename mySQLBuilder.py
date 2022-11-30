import mysql.connector
from mysql.connector import errorcode
import glob
import json
import os
import requests
import base64

#This script builds MySQL tables that do not exist
#Checks the JSON folder for artists that don't exist in the artist table
#Then adds them into the table schema
#It updates existing entries

src = "json/*"
imgsrc = "images/"
imagetypes = ["Portrait", "Landscape", "Still Life", "Full Body", "City"]
imagenum = 4
HOST = "http://asherquazar.local"
MEDIA = HOST + "/wp-json/wp/v2/media"
PASS = "Ta3x qPGC mgkr mt4X H7f1 si3i"
USER = "tlalka"

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
    "  image_id int NOT NULL,"
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
    resp = cursor.fetchone()
    artistID = -1

    if(resp is not None):
        print(dupe_data[0] + " exists. Updating.")
        add_artist=("UPDATE artist_index SET birth=%s, death=%s, style=%s, bio=%s, wiki=%s, nationality=%s"
            "WHERE name=%s")
        data_artist = (data['birthyear'], data['deathyear'], data['style'], data['biography'], data['wikilink'], data['nationality'], data['fullname'])
        cursor.execute(add_artist, data_artist)
        artistID = resp[0]
        print(cursor.rowcount, "record(s) affected") 

    else:
        print(dupe_data + " does not exist. adding.")
        add_artist=("INSERT INTO artist_index"
            "(name, birth, death, style, bio, wiki, nationality)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s)")
        data_artist = (data['fullname'], data['birthyear'], data['deathyear'], data['style'], data['biography'], data['wikilink'], data['nationality'])
        #cursor.execute(add_artist, data_artist)
        artistID = cursor.lastrowid
        #connection.commit()
    
    #Add image to wordpress. Image names are standardized
    #If image exists in folder upload it to wordpress replacing the old one
    #If it doesnt, pull the wordpress ID for the existing one
    tmppath = imgsrc + data['fullname'].replace(" ", "_")
    dirList = os.listdir(tmppath)
    
    #Run through image types
    for dir in imagetypes:
        tmppath2 = tmppath + "/" + dir.replace(" ", "_")
        #Run though number of images
        for i in range(1, imagenum + 1):
            tmpfile = tmppath2 + "/" + dir.replace(" ", "-") + "-in-the-style-of-" + data['fullname'].replace(" ", "-") + "-" + str(i) + ".png"
            filename = os.path.basename(tmpfile)

            #if image exists locally, upload/update it on WP
            if os.path.exists(tmpfile):
                image = open(tmpfile, "rb").read()
                credentials = USER + ':' + PASS
                token = base64.b64encode(credentials.encode())

                #test if image exists on WP by checking the name
                param = {
                    'search': filename
                    }
                
                resp = requests.get(
                    MEDIA,
                    params = param
                )

                #print(resp)
                newDict = resp.json()
                #print(newDict)

                if len(newDict) > 0:
                    imageID = newDict[0]['id']
                    print(filename + " exists at ID " + str(imageID))
                    #Image exists on WP, so update it
                    header = {
                        'Authorization': 'Basic ' + token.decode('utf-8')
                    }

                    resp = requests.post(
                        MEDIA + "/" + str(imageID),
                        headers = header,
                        data = image,
                    )
                    
                    #print(resp)
                    newDict = resp.json()
                    #print (newDict)
                    link = newDict.get('guid').get("rendered")
                    print ("UPDATED at " + link)

                else:
                    #image does not exist on WP, so upload it
                    print(filename + " does not exist")
                    header = {
                        'Authorization': 'Basic ' + token.decode('utf-8'),
                        "Content-Type": 'image/png',
                        'Content-Disposition': 'attachment; filename=' + filename,
                    }

                    resp = requests.post(
                        MEDIA,
                        headers = header,
                        data = image,
                    )
                    
                    newDict = resp.json()
                    imageID = newDict.get('id')
                    link = newDict.get('guid').get("rendered")
                    print ("ADDED new image at at " + link)

            #Image uploaded/updated - add it to image index
            #Check for duplicate
            check_dupe=("SELECT artist_id from image_index WHERE image_id=%s")
            dupe_data=(imageID,)
            cursor.execute(check_dupe, dupe_data)    
            resp = cursor.fetchone()
            print(resp)

            if(resp is not None):
                print(str(imageID) + " exists. Updating.")
                add_image=("UPDATE image_index SET artist_id=%s, type=%s WHERE image_id=%s")
                data_image = (artistID, dir, imageID)
                cursor.execute(add_image, data_image)
                print(cursor.rowcount, "record(s) affected") 

            else:
                print(str(imageID) + " does not exist. Adding.")
                add_image=("INSERT INTO image_index"
                    "(image_id, artist_id, type)"
                    "VALUES (%s, %s, %s)")
                data_image = (imageID, artistID, dir)
                cursor.execute(add_image, data_image)
                emp_no = cursor.lastrowid
                print(emp_no)
                #connection.commit()
            break
        break


    #image_index
    add_image=(
        "INSERT INTO image_index"
        "(image_id, artist, type)"
        "VALUES (%i, %i, %s)"
    )
    #data_artist = (XXX, data['fullname'], data['deathyear'])
    
    #image_type_details
    #decades
    #movements
    #tags
    #pitfalls
    connection.commit()

if __name__ == '__main__':
    main()