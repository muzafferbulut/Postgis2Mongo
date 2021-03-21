# -*- coding: utf-8 -*-
"""
Created on Thu Mar 18 20:08:49 2021

@author: Muzaffer
"""

import psycopg2 as pg
import json
from pymongo import MongoClient

def ConnectToPGDatabase(configFile): # connect to pg database
    conn = pg.connect(
        host = configFile['host'],
        database = configFile['database'],
        user = configFile['user'],
        password = configFile['password']
        )
    return conn

def ConnectToMongoDB(): # connect to mongodb database
    # Default localhost
    client = MongoClient()
    return client

def OpenJson(path): # open .json file
    with open(path,"r") as file:
        JsonFile = json.load(file)
    return JsonFile

def QueryBuild(ColumnList): # build query from pg database
    query=""
    for i in range(len(TableInfo)-2):
        query = query+ ", " + ColumnList[i]
    query = query.replace(",","",1)
    query = "SELECT" + query + ", st_asgeojson("+ TableInfo['GeometryColumn']+ ")"+ " FROM " + TableInfo['Table']
    return query

def BuildDict(ColumnList,PgTable): # convert query to dict
    RecordDict = {}
    [x,y] = len(PgTable),len(PgTable[0])
    for i in range(x):
        for j in range(y):
            RecordDict[ColumnList[j]] = PgTable[i][j]
    return RecordDict

def BuildColumnList(TableInfo):
    listLength = len(TableInfo)-2
    ColumnList = []
    counter =1
    for i in range(listLength):
        ColumnList.append(TableInfo["Column"+str(counter)])
        counter +=1
    ColumnList.append(TableInfo["GeometryColumn"])
    return ColumnList

ConfigFile = OpenJson("Pg_config.json") # read file
TableInfo = OpenJson("Pg_table.json") # read file
MongoInfo  = OpenJson("MongoInfo.json")

# connect pg database
conn = ConnectToPGDatabase(ConfigFile)
cur = conn.cursor()

# pg column list
ColumnList = BuildColumnList(TableInfo)

# query building
query = QueryBuild(ColumnList)
cur.execute(query)
PgTable = cur.fetchall()

client = ConnectToMongoDB()

db = client.MongoInfo["database"]

col = db.MongoInfo["collection"]

for i in range(len(PgTable)):
    record = BuildDict(ColumnList,PgTable)
    x = col.insert_one(record)
    
print("Finish.")
