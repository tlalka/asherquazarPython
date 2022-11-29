import mysql.connector
from mysql.connector import errorcode
import glob
import json
import os
import requests
import base64


url = "http://asherquazar.local/wp-json/wp/v2/posts"
user = "tlalka"
password = "Ta3x qPGC mgkr mt4X H7f1 si3i"
credentials = user + ':' + password
token = base64.b64encode(credentials.encode())
header = {'Authorization': 'Basic ' + token.decode('utf-8')}
responce = requests.get(url , headers=header)
print(responce)

resp = requests.post(
        "http://asherquazar.local/wp-json/wp/v2/media",
        headers = {"Authorization": "Bearer Ta3x qPGC mgkr mt4X H7f1 si3i",
        "Content-Type": 'doc/txt',
        'Content-Disposition': 'attachment; filename=test'},
        data = "ass")
print(resp)


