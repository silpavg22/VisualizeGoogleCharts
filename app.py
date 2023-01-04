# Importing flask module in the project is mandatory
# An object of Flask class is our WSGI application.
import os
import pandas as pd
from flask import Flask, render_template, request
from math import sin, cos, sqrt, atan2, radians
import random
import redis
import pyodbc
import csv
import time
import ast
import MySQLdb
from geographiclib.geomath import Math

app = Flask(__name__)
app.config.from_pyfile('config.py')

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
# print(r)
server = ""
database = ""
username = ""
password = ""

cnxn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
    + server
    + ";DATABASE="
    + database
    + ";UID="
    + username
    + ";PWD="
    + password
)
cursor = cnxn.cursor()



@app.route('/', methods=['GET', 'POST'])
def getHome():
    return render_template('index.html')

@app.route('/piechart', methods=['GET'])
def select():
    start = int(round(time.time() * 1000))
    qry = """select t.range as [score range], count(*) as [number of occurences]
            from (
              select case  
                when mag between 0 and 1 then '0 - 1'
                when mag between 1 and 2 then '1 - 2'
                when mag between 2 and 3 then '2 - 3'
                else '3 - 5' end as range
              from projectDB..earthquake) t
            group by t.range""";
    cursor.execute(qry)
    result = cursor.fetchall()
    labels = []
    values = []
    for d in result:
        values.append(d[0])
        labels.append(d[1])

    end = int(round(time.time() * 1000))
    totaltime = end - start
    res = list(map(list, result))
    insert_list = ["Magnitude Range", "Count"]
    res.insert(0,insert_list)
    return render_template('piechart.html', reslt1=res, t1=totaltime)


@app.route('/scatterchart', methods=['GET'])
def scatterplot():
    noofrows = request.args.get('noofrows')
    start = int(round(time.time() * 1000))
    qry = "select mag,depth from  projectDB..earthquake order by time desc offset 0 rows fetch next "+noofrows+" rows only"
    cursor.execute(qry)
    result = cursor.fetchall()

    end = int(round(time.time() * 1000))
    totaltime = end - start
    res = list(map(list, result))
    insert_list = ["Magnitude", "Depth"]
    res.insert(0,insert_list)
    return render_template('scatterchart.html', reslt1=res, t1=totaltime)

@app.route('/columnchart', methods=['GET'])
def columnchart():
    noofrows = request.args.get('noofrows')
    start = int(round(time.time() * 1000))
    qry = "select mag,depth,place from  projectDB..earthquake order by time desc offset 0 rows fetch next "+noofrows+" rows only"
    cursor.execute(qry)
    result = cursor.fetchall()
    data = []
    for d in result:
        color = '#b87333'
        rowAsList = list(d)
        rowAsList.append(color)
        data.append(rowAsList)
    end = int(round(time.time() * 1000))
    totaltime = end - start
    insert_list = ["Magnitude", "Depth", { 'role': 'annotation' },{ 'role': 'style' }]
    data.insert(0,insert_list)
    return render_template('columnchart.html', reslt1=data, t1=totaltime)


@app.route('/barchart', methods=['GET'])
def barchart():
    noofrows = request.args.get('noofrows')
    start = int(round(time.time() * 1000))
    qry = "select mag,depth,mag from  projectDB..earthquake order by time desc offset 0 rows fetch next "+noofrows+" rows only"
    cursor.execute(qry)
    result = cursor.fetchall()

    end = int(round(time.time() * 1000))
    totaltime = end - start
    res = list(map(list, result))
    insert_list = ["Magnitude", "Depth",{ 'role': 'annotation' }]
    res.insert(0,insert_list)
    return render_template('barchart.html', reslt1=res, t1=totaltime)



@app.route('/scatterchartcus', methods=['GET'])
def scatterfromtoyear():
    msg = ""
    from_year = request.args.get('from_year')
    to_year = request.args.get('to_year')
    statecode = (request.args.get('statecode'))
    start = int(round(time.time() * 1000))
    qry = "select year, count(votes) from projectDB..pvotes where year >= "+from_year+" and year <= "+to_year+" and state='"+statecode+"' group by year ";
    cursor.execute(qry)
    result = cursor.fetchall()
    str='Year vs. Total Votes comparison'
    end = int(round(time.time() * 1000))
    totaltime = end - start
    res = list(map(list, result))
    insert_list = ["Year", "Total Votes"]
    return render_template('scatterchartcus.html', reslt1=res, title=str)

@app.route('/barchartcus', methods=['GET'])
def barchartcus():
    msg = ""
    from_year = request.args.get('from_year')
    to_year = request.args.get('to_year')
    statecode = (request.args.get('statecode'))
    start = int(round(time.time() * 1000))
    qry = "select  year,count(party),party from projectDB..pvotes  where year > ="+from_year+" and year <= "+to_year+" and state='"+statecode+"' group by year,party order by year desc";
    cursor.execute(qry)
    result = cursor.fetchall()
    end = int(round(time.time() * 1000))
    totaltime = end - start
    resdict = {}
    #data = [['Year', 'other', 'dem', 'LIB']]
    data = []
    for res in result:
        if res[0] in resdict:
            resdict[res[0]].append(res[1])
            resdict[res[0]].append(res[2])
        else:
            resdict[res[0]] = [str(res[0]), res[1],res[2]]
    for value in resdict.values():
        data.append(value)

    insert_list = ["Year","DEM",{ 'role': 'annotation'}, "LIB",{ 'role': 'annotation'},"OTHER",{ 'role': 'annotation'},"REP",{ 'role': 'annotation'}]
    data.insert(0, insert_list)
    return render_template('barchartcus.html', reslt1=data, t1=totaltime)


@app.route('/linechart', methods=['GET'])
def linechart():
    msg = ""
    from_year = request.args.get('from_year')
    to_year = request.args.get('to_year')
    statecode = (request.args.get('statecode'))
    start = int(round(time.time() * 1000))
    qry = "select  year,count(party) from projectDB..pvotes  where year > ="+from_year+" and year <= "+to_year+" and state='"+statecode+"' group by year ";
    cursor.execute(qry)
    result = cursor.fetchall()
    end = int(round(time.time() * 1000))
    totaltime = end - start
    res = list(map(list, result))
    insert_list = ["Year", "Total Votes"]
    res.insert(0,insert_list)
    return render_template('linechart.html', reslt1=res, t1=totaltime)

@app.route('/piechartcus', methods=['GET'])
def piechartcus():
    msg = ""
    year = request.args.get('year')
    statecode = (request.args.get('statecode'))
    start = int(round(time.time() * 1000))
    qry = "Select party,count(*) from projectDB..pvotes where year  ="+year+" and state='"+statecode+"' group by party";
    cursor.execute(qry)
    result = cursor.fetchall()
    end = int(round(time.time() * 1000))
    totaltime = end - start
    res = list(map(list, result))
    insert_list = [ "Total Votes","Party"]
    res.insert(0,insert_list)
    return render_template('piechartcus.html', reslt1=res, t1=totaltime)




if __name__ == "__main__":
    app.run(host="0.0.0.0")
