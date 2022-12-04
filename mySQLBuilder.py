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

#Keep bio info on an artist
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

#map images WP ids to artists and their type
TABLES['image_index'] = (
    "CREATE TABLE image_index ("
    "  image_id int NOT NULL,"
    "  artist_id int NOT NULL,"
    "  type varchar (25) NOT NULL,"  
    "  PRIMARY KEY (image_id)"
    ") ENGINE=InnoDB")

#map images WP ids to artists and their type
TABLES['works_index'] = (
    "CREATE TABLE works_index ("
    "  image_id int NOT NULL,"
    "  artist_id int NOT NULL," 
    "  PRIMARY KEY (image_id)"
    ") ENGINE=InnoDB")

#map images WP ids to artists and their type
TABLES['profile_index'] = (
    "CREATE TABLE profile_index ("
    "  image_id int NOT NULL,"
    "  artist_id int NOT NULL," 
    "  PRIMARY KEY (image_id)"
    ") ENGINE=InnoDB")

#record description of each image type per artist
TABLES['image_type_details'] = (
    "CREATE TABLE image_type_details ("
    "  artist_id int NOT NULL,"
    "  type varchar (25) NOT NULL,"
    "  description text"
    ") ENGINE=InnoDB")

#record decades an artist was active
TABLES['decades'] = (
    "CREATE TABLE decades ("
    "  artist_id int NOT NULL,"
    "  year smallint NOT NULL"
    ") ENGINE=InnoDB")

#record movements an artist participated in
TABLES['movements'] = (
    "CREATE TABLE movements ("
    "  artist_id int NOT NULL,"
    "  movement varchar (25) NOT NULL"
    ") ENGINE=InnoDB")

#record tags associated with an artist
TABLES['tags'] = (
    "CREATE TABLE `tags` ("
    "  artist_id int NOT NULL,"
    "  tag varchar (25) NOT NULL"
    ") ENGINE=InnoDB")

#record pitfalls associated with an artist
TABLES['pitfalls'] = (
    "CREATE TABLE pitfalls ("
    "  artist_id int NOT NULL,"
    "  pitfall varchar (25) NOT NULL"
    ") ENGINE=InnoDB")

#-----------------MAIN
def main():
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
            create_tables(cursor)

            #Load up JSONs
            loop_JSON(cursor, connection)

    except errorcode as e:
        print("Error while connecting to MySQL", e)

#---------------DISCONNECT    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

#-----------------------FUNCTIONS
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
            print("JSON handle: " + data['fullname'])
            add_one_JSON(cursor, data, connection) 

        connection.commit()

        
       
#!!!!!!!!!!!!probably need to reset cursor here            
def add_one_JSON(cursor, data, connection):
    #add one JSON to the database

    #artist_index. Check if artist name exists
    filename = data['fullname']
    table_name = "artist_index"
    artistID = -1
    dupe_ID = "name"
    dupe_val = data['fullname']
    column_IDs = ["birth", "death", "style", "bio", "wiki", "nationality"]
    column_vals = [str(data['birthyear']), str(data['deathyear']), data['style'], data['biography'], data['wikilink'], data['nationality']]
    artistID = mySQL_add_or_update(table_name, artistID, dupe_ID, dupe_val, column_IDs, column_vals, cursor)

    #add profile and works images if they exist
    imagespath = imgsrc + data['fullname'].replace(" ", "_") + "/works"
    for image in os.listdir(imagespath):
        if (image.endswith(".png") or image.endswith(".jpg")):
            if(image.startswith("profile")):
                #upload profile. There should be only one per
                imageID = WPImage_add_or_update(data['fullname'] + "_" + image, imagespath + "/" + image)
                table_name = "profile_index"
                dupe_ID = "artist_id"
                dupe_val = artistID
                column_IDs = ["image_id"]
                column_vals = [imageID]
                mySQL_add_or_update(table_name, artistID, dupe_ID, dupe_val, column_IDs, column_vals, cursor)

            else:
                #upload works. Image IDs should only appear once
                imageID = WPImage_add_or_update(data['fullname'] + "_" + image, imagespath + "/" + image)
                table_name = "works_index"
                dupe_ID = "image_id"
                dupe_val = imageID
                column_IDs = ["artist_id"]
                column_vals = [artistID]
                mySQL_add_or_update(table_name, artistID, dupe_ID, dupe_val, column_IDs, column_vals, cursor)

    return 1
    #decades
    for decade in data['decades']:
        table_name = "decades"
        dupe_ID = "year"
        dupe_val = decade
        column_IDs = ['artist_id']
        column_vals = [artistID]
        mySQL_add_or_update(table_name, artistID, dupe_ID, dupe_val, column_IDs, column_vals, cursor)

    #movements  
    for movement in data['movements']:
        table_name = "movements"
        dupe_ID = "movement"
        dupe_val = movement
        column_IDs = ['artist_id']
        column_vals = [artistID]
        mySQL_add_or_update(table_name, artistID, dupe_ID, dupe_val, column_IDs, column_vals, cursor)

    #tags      
    for tag in data['tags']:
        table_name = "tags"
        dupe_ID = "tag"
        dupe_val = tag
        column_IDs = ['artist_id']
        column_vals = [artistID]
        mySQL_add_or_update(table_name, artistID, dupe_ID, dupe_val, column_IDs, column_vals, cursor)

    #pitfalls
    for pitfall in data['pitfalls']:
        table_name = "pitfalls"
        dupe_ID = "pitfall"
        dupe_val = pitfall
        column_IDs = ['artist_id']
        column_vals = [artistID]
        mySQL_add_or_update(table_name, artistID, dupe_ID, dupe_val, column_IDs, column_vals, cursor)

    #Add image to wordpress. Image names are standardized
    #If image exists in folder upload it to wordpress replacing the old one
    #If it doesnt, pull the wordpress ID for the existing one
    tmppath = imgsrc + data['fullname'].replace(" ", "_")
    dirList = os.listdir(tmppath)
    
    #Run through image types
    for dir in imagetypes:
        tmppath2 = tmppath + "/" + dir.replace(" ", "_")
        #Update image_type_details. Method does not work for this because we have 2 wheres
        check_dupe=("SELECT artist_id from image_type_details WHERE type=%s")
        dupe_data=(dir,)
        cursor.execute(check_dupe, dupe_data)    
        resp = cursor.fetchall()

        if(resp is not None and ((artistID,) in resp)):
            print(dir + " exists in image_type_details. Updating.")
            add_image=("UPDATE image_type_details SET description=%s WHERE type=%s AND artist_id=%s")
            data_image = (data[dir.replace(" ", "_").lower()], dir, artistID)
            cursor.execute(add_image, data_image)
            print(cursor.rowcount, "record(s) affected") 

        else:
            print(dir + " does not exist in image_type_details. Adding.")
            add_image=("INSERT INTO image_type_details"
                "(artist_id, description, type)"
                "VALUES (%s, %s, %s)")
            data_image = (artistID, data[dir.replace(" ", "_").lower()], dir)
            cursor.execute(add_image, data_image)
            emp_no = cursor.lastrowid

        #Run though number of images
        for i in range(1, imagenum + 1):
            tmpfile = tmppath2 + "/" + dir.replace(" ", "-") + "-in-the-style-of-" + data['fullname'].replace(" ", "-") + "-" + str(i) + ".png"
            filename = os.path.basename(tmpfile)

            #if image exists locally, upload/update it on WP
            if os.path.exists(tmpfile):

                imageID = WPImage_add_or_update(filename, tmpfile)

                #Image uploaded/updated - add it to image index
                if(imageID == -1):
                    raise ValueError("imageID was not set")
                
                table_name = "image_index"
                dupe_ID = "image_id"
                dupe_val = imageID
                column_IDs = ['artist_id', 'type']
                column_vals = [artistID, dir]
                mySQL_add_or_update(table_name, artistID, dupe_ID, dupe_val, column_IDs, column_vals, cursor)
            

def mySQL_add_or_update(table_name, artistID, dupe_ID, dupe_val, column_IDs, column_vals, cursor):
    #Take in table_name, artistID, dupe_ID, dupe_val, column_IDs[], column_vals[], cursor
    #Everything must come in as a string except artist ID
    
    if (len(column_IDs) != len(column_vals)):
        raise ValueError("column ID and vals must have the same size")
    
    # values use %s to add quotes, but IDs must be added directly to the string
    check_dupe=("SELECT artist_id FROM " + table_name + " WHERE " + dupe_ID + "=%s")
    dupe_data=(dupe_val,)
    cursor.execute(check_dupe, dupe_data)    
    #resp = cursor.fetchone()
    resp = cursor.fetchall()
    print(check_dupe % dupe_data)
    newID = -1

    #if we are running the insert artist, artist ID passed in will be -1 and cannot be used for checks
    #new ID is only collected in this case
    if(artistID == -1):
        if(resp is not None):
            artistID = resp[0][0]
            print("overide artist ID")

    newID = artistID

    if(resp is not None and ((artistID,) in resp)):
        if(len(column_IDs) > 1):
            print("artistID=" + str(artistID) + " and " + dupe_ID + "=" + str(dupe_val) + " exists in " + table_name + ". Updating.")
            add_string = "UPDATE " + table_name + " SET "
            add_data = ()
            for ID, val in zip(column_IDs, column_vals):
                add_string = add_string + ID + "=%s, "
                add_data = add_data + (val,)
            add_string = add_string[:-2] + " WHERE " + dupe_ID + "=%s"
            add_data = add_data + (dupe_val,)
            print(add_string % add_data)
            cursor.execute(add_string, add_data)
            print(cursor.rowcount, "record(s) affected")
        else:
            print("artistID=" + str(artistID) + " and " + dupe_ID + "=" + str(dupe_val) + " exists in " + table_name + ". Doing nothing.")

    else:
        print("artistID=" + str(artistID) + " and " + dupe_ID + "=" + str(dupe_val) + " does not exist in " + table_name + ". Adding.")

        add_string = "INSERT INTO " + table_name + " ("
        add_data = ()
        vals = "VALUES ("
        for ID, val in zip(column_IDs, column_vals):
            add_string = add_string + ID + ", "
            add_data = add_data + (val,)
            vals = vals + "%s, "

        add_string = add_string + dupe_ID + ") " + vals + "%s )"
        add_data = add_data + (dupe_val,)
        print(add_string % add_data)
        cursor.execute(add_string, add_data)
        newID = cursor.lastrowid
    return newID

    
def WPImage_add_or_update(wpfilename, localfile):
    print("upload/update " + localfile)
    image = open(localfile, "rb").read()
    credentials = USER + ':' + PASS
    token = base64.b64encode(credentials.encode())
    imageID = -1

    #test if image exists on WP by checking the name
    param = {
        'search': wpfilename
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
        print(wpfilename + " exists at ID " + str(imageID))
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
        print(wpfilename + " does not exist")
        header = {
            'Authorization': 'Basic ' + token.decode('utf-8'),
            "Content-Type": 'image/png',
            'Content-Disposition': 'attachment; filename=' + wpfilename,
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
    return imageID


if __name__ == '__main__':
    main()
