from pymongo import MongoClient
import os
# client = MongoClient('localhost', 27017)
# database_url = 
client = MongoClient('mongodb://localhost:27017')
db = client['NBA']

