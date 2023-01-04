# Importing flask module in the project is mandatory
# An object of Flask class is our WSGI application.
import os
import pandas as pd
from flask import Flask, render_template, request
from math import sin, cos, sqrt, atan2, radians

import redis
import pyodbc
import csv
import time
import ast
import MySQLdb


app = Flask(__name__)
app.config.from_pyfile('config.py')

#r = redis.Redis(host='hostname',port=port,password='password')
r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
# r = redis.Redis(host="redisaws.lbbznu.ng.0001.use2.cache.amazonaws.com", port=6379, db=0,socket_timeout=10,decode_responses=True)
print(r)
server = ""
database = "projectDB"
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

@app.route('/finddetails', methods=['GET'])
def getearth():
    msg = ""
    val = request.args.get('magnitude')
    itr1 = int(request.args.get('itr1'))
    if (validateInput(val, "Magnitude") == ''):
        start = int(round(time.time() * 1000))
        for i in range(itr1):
            qry = "select count(*)  FROM projectDB..earthquake WHERE MAG > " + val + " ";
            cursor.execute(qry)
            result = cursor.fetchall()
            r.set("cntmag",str(result))
        end = int(round(time.time() * 1000))
        totaltime = end - start
        return render_template('index.html', reslt1=result[0], t1=totaltime)
    else:
        msg = "Enter magnitude"
        return render_template('index.html', errormsg1=msg)


@app.route('/findtimerandom', methods=['GET'])
def findtimerandom():
    if request.method == 'GET':
        rows=[]
        start = int(round(time.time() * 1000))
        qry1="Select mag, place from projectDB..earthquake where place like '%ca%' group by place,mag"
        cursor.execute(qry1)
        rows=cursor.fetchall()
        r.set("qry1",str(rows))
        qry2="Select mag, place, count(*) from projectDB..earthquake group by place,mag having count(*)>2"
        cursor.execute(qry2)
        rows = cursor.fetchall()
        r.set("qry2", str(rows))
        qry3="Select mag, place,net, count(*) from projectDB..earthquake where net='nn' group by place,mag,net"
        cursor.execute(qry3)
        rows = cursor.fetchall()
        r.set("qry3", str(rows))
        qry4 = "select count(*) from projectDB..earthquake where SUBSTRING(CAST(time AS VARCHAR(19)), 12, 2)> = 20  and SUBSTRING(CAST(time AS VARCHAR(19)), 12, 2)<=8 and mag>4"
        cursor.execute(qry4)
        rows = cursor.fetchall()
        r.set("qry4", str(rows))
        qry5="	select count(*),net  from projectDB..earthquake where place like '%SE%' group by net"
        cursor.execute(qry5)
        rows = cursor.fetchall()
        r.set("qry5", str(rows))
        end = int(round(time.time() * 1000))
        totaltime = end - start
    return render_template('index.html', trand=totaltime)

@app.route('/findtimerandomcache', methods=['GET'])
def findtimerandomcache():
    if request.method == 'GET':
        start = int(round(time.time() * 1000))
        if r.get("qry1"):
            rows1=ast.literal_eval(r.get("qry1"))
        if r.get("qry2"):
            rows2 = ast.literal_eval(r.get("qry2"))
        if r.get("qry3"):
            rows3 = ast.literal_eval(r.get("qry3"))
        if r.get("qry4"):
            rows4 = ast.literal_eval(r.get("qry4"))
        if r.get("qry5"):
            rows4 = ast.literal_eval(r.get("qry5"))

        end = int(round(time.time() * 1000))
        totaltime = end - start
    return render_template('index.html', trand=rows4, trandtime=totaltime)

@app.route('/createdatabase', methods=['GET'])
def createdatabase():
    columns=["Name","Branch","Roll","Section"]
    # sql = """CREATE TABLE STUDENT1 (
    #                        NAME  VARCHAR(20) NOT NULL,
    #                        BRANCH VARCHAR(50),
    #                        ROLL INT NOT NULL,
    #                        SECTION VARCHAR(5),
    #                        AGE INT
    #                        )"""
    columns = " VARCHAR(250), ".join(columns)
    sql = "CREATE TABLE Customers1 (" + columns + " VARCHAR(250))"
    cursor.execute(sql)
    cursor.commit()
    return render_template('index.html')



@app.route('/replacedetails', methods=['GET'])
def replacedetails():
        netorg = str(request.args.get('netorg'))
        netrep = str(request.args.get('netrep'))
        sql = "UPDATE projectDB..earthquake SET net  = '"+netrep+"' WHERE net ='"+netorg+"'"

        cursor.execute(sql)
        cursor.commit()
        return render_template('index.html')


@app.route('/finddetailscache', methods=['GET'])
def finddetailscache():
        value=[]
        val = request.args.get('magnitude')
        itr2 = int(request.args.get('itr2'))
        if(validateInput(val,"Magnitude")==''):
            start = int(round(time.time() * 1000))
            for i in range(itr2):
                if(r.get("cntmag")):
                    res=r.get("cntmag")
                    x = ast.literal_eval(res)
            end = int(round(time.time() * 1000))
            totaltime = end - start
            return render_template('index.html',t2=totaltime, reslt2=x[0])
        else:
            msg="Enter magnitude"
            if ( not r.get("cntmag")):
                msg=" No records found in Cache..!!"
            return render_template('index.html',errormsg2=msg)


@app.route('/findquakewithin', methods=['GET'])
def findEarthquakelist():
    if request.method == 'GET':
        long1 = request.args.get('longitude')
        lat1 = request.args.get('latitude')
        inptdist = request.args.get('distance')
        itr3 = int(request.args.get('itr3'))

        if(validateInput(long1,"")=='' or validateInput(lat1,"")=='' or validateInput(inptdist,"")==''):
            i=0;
            rows=[]
            start = int(round(time.time() * 1000))
            for i in range(itr3):
                qry="select longitude, latitude FROM projectDB..earthquake ";
                cursor.execute(qry)
                rows=cursor.fetchall()
            end = int(round(time.time() * 1000))
            totaltime = end - start
            r.set("earthwithin",str(rows))
            for row in rows:
               long2=row[0]
               lat2=row[1]
               dist=findDistance(lat1, long1, lat2, long2);
               print("The distance is %.2fkm." % dist)
               msg = "No earthquakes found"
               if dist < int(inptdist):
                    i=i+1
            if(not i==0):
                return render_template('index.html', reslt3=i,t3=totaltime)
            else:
                msg="No earthquakes found"
                return render_template('index.html', errormsg3=msg)
        else:
            msg="Enter input values"
            return render_template('index.html', errormsg3=msg)



@app.route('/findquakewithincache', methods=['GET'])
def findquakewithincache():
    if request.method == 'GET':
        long1 = request.args.get('longitude')
        lat1 = request.args.get('latitude')
        inptdist = request.args.get('distance')
        itr4 = int(request.args.get('itr4'))
        msg=''
        if(validateInput(long1,"")=='' or validateInput(lat1,"")=='' or validateInput(inptdist,"")==''):
            i=0;
            rows=[]
            start = int(round(time.time() * 1000))
            if(r.get("earthwithin")):
                for i in range(itr4):
                    values=ast.literal_eval(r.get("earthwithin"))
            end = int(round(time.time() * 1000))
            totaltime = end - start
            if(values):
                for row in values:
                   long2=row[0]
                   lat2=row[1]
                   dist=findDistance(lat1, long1, lat2, long2);
                   print("The distance is %.2fkm." % dist)
                   msg = "No earthquakes found"
                   if dist < int(inptdist):
                        i=i+1
            else:
                msg="No records found in Cache..!!"
            if(not i==0):
                return render_template('index.html', reslt4=i,t4=totaltime)
            else:
                msg="No earthquakes found"
                return render_template('index.html', errormsg4=msg)
        else:
            msg="Enter input values"
            return render_template('index.html', errormsg4=msg)


@app.route('/getclosestmag', methods=['GET'])
def getclosestmag():
    if request.method == 'GET':
        msg=""
        long1 = request.args.get('longitude')
        lat1 = request.args.get('latitude')
        itr5 = int(request.args.get('itr5'))
        if (validateInput(long1, "") == '' or validateInput(lat1, "") == ''):
            start = int(round(time.time() * 1000))
            for i in range(itr5):
                qry="select latitude as lat, longitude as long,  place as place, mag as mag , time as time FROM projectDB..earthquake WHERE MAG>6 ";
                cursor.execute(qry)
                rows = cursor.fetchall()
                minRow = rows[0]
            end = int(round(time.time() * 1000))
            totaltime = end - start
            minDist = findDistance(lat1, long1, minRow[0], minRow[1]);
            for row in rows:
                dist=findDistance(lat1, long1, row[0], row[1]);
                if(minDist > dist):
                    minDist=dist
                    minRow=row
                print("Minrow", minRow)
            if(minRow):
                return render_template('index.html', reslt5=minRow, t5=totaltime)
            else:
                msg="No records found!!"
                return render_template('index.html', errormsg5=msg)
        else:
            msg="Enter longitude and latitude"
            return render_template('index.html', errormsg5=msg)



@app.route('/findlargestquakelastweek', methods=['GET'])
def findlargestquaklastweek():
    if request.method == 'GET':
        rows=[]
        itr6 = int(request.args.get('itr6'))
        nodays = (request.args.get('nodays'))
        start = int(round(time.time() * 1000))
        for i in range(itr6):
            qry="select mag as magnitude, latitude as lat, longitude as long ,place as place from projectDB..earthquake where time between GETDATE()-"+nodays+" AND GETDATE()";
            cursor.execute(qry)
            rows =cursor.fetchall()
        end = int(round(time.time() * 1000))
        totaltime = end - start
        max=0;
        maxrow=[]
        long1 = request.args.get('longitude1')
        lat1 = request.args.get('latitude1')
        if (validateInput(long1, "") == '' or validateInput(lat1, "") == ''):
            distancelastweek=request.args.get('distancelastweek')
            for row in rows:
                dist = findDistance(lat1, long1, row[1], row[2]);
                if(dist > int(distancelastweek)):
                    if(max < row[0]):
                        max = row[0]
                        maxrow=row
            if(maxrow):
                return render_template('index.html', reslt6=maxrow, t6=totaltime)
            else:
                msg="No records found!!"
                return render_template('index.html', errormsg6=msg)
        else:
            msg="Enter longitude and latitude"
            return render_template('index.html', errormsg6=msg)

@app.route('/findNquakes', methods=['GET'])
def findNquakes():
    if request.method == 'GET':
        val = request.args.get('nooofquakes')
        itr7 = int(request.args.get('itr7'))

        if(validateInput(val,"Number of Earthquakes")==''):
            start = int(round(time.time() * 1000))
            for i in range(itr7):
                qry="select * from projectDB..earthquake ORDER BY mag offset 0 rows fetch next "+ val +" rows only"
                cursor.execute(qry)
                rows=cursor.fetchall()
                r.set("largeNqukes",str(rows))
            end = int(round(time.time() * 1000))
            totaltime = end - start
            nquakes = []
            for row in rows:
                nquakes.append(row)
            print(nquakes)
            if(nquakes):
                return render_template('index.html', reslt7=nquakes,t7=totaltime)
            else:
                msg="No records found!!"
                return render_template('index.html', errormsg5=msg)
        else:
            msg="Number of Earthquakes"
            return render_template('index.html', errormsg5=validateInput(val,msg=msg))

@app.route('/findNquakescache', methods=['GET'])
def findNquakescache():
    if request.method == 'GET':
        val = request.args.get('nooofquakes')
        itr8 = int(request.args.get('itr8'))

        if(validateInput(val,"Number of Earthquakes")==''):
            if r.get("largeNqukes"):
                start = int(round(time.time() * 1000))
                for i in range(itr8):
                    rows = ast.literal_eval(r.get("largeNqukes"))
                end = int(round(time.time() * 1000))
                totaltime = end - start
            else:
                msg="No records in Cache"
            if(rows):
                return render_template('index.html', reslt8=rows,t8=totaltime,errormsg8=msg)
            else:
                msg="No records found!!"
                return render_template('index.html', errormsg8=msg)
        else:
            msg="Number of Earthquakes"
            return render_template('index.html', errormsg8=validateInput(val,msg=msg))

@app.route('/findquakesbtwmag', methods=['GET'])
def findquakesbtwmag():
    if request.method == 'GET':
        fromMag = request.args.get('from_mag')
        toMag = request.args.get('to_mag')
        noofdays=request.args.get('nodays')
        if(validateInput(fromMag,"")=='' or validateInput(toMag,"")=='' ):
            if(noofdays and noofdays.isnumeric()):
                qry="SELECT * FROM  projectDB..earthquake where mag > ='"+fromMag+"' and mag < = '"+toMag+"' and  time between GETDATE()-"+noofdays+" AND GETDATE()"
            else:
                 qry = "SELECT * FROM  projectDB..earthquake where mag > ='" + fromMag + "' and mag < = '" + toMag + "' "
            cursor.execute(qry)
            res = []
            for row in cursor:
                res.append(row)
            if(res):
                return render_template('index.html', magres=res)
            else:
                msg = "No records found!!"
                return render_template('index.html', errormsg6=msg)
        else:
            msg="Enter input values"
            return render_template('index.html',errormsg6=msg)


@app.route('/findquakesbtwfronandto', methods=['GET'])
def findquakesbtwfronandto():
    if request.method == 'GET':
        fromDate = request.args.get('from_date')
        toDate = request.args.get('to_date')
        if(validateInput(fromDate,'')=='' or validateInput(toDate,'')==''):
            qry="SELECT * FROM  projectDB..earthquake where SUBSTRING(CAST(time AS VARCHAR(19)), 0,11) BETWEEN '"+fromDate+"' and '"+toDate+"' and mag>3"
            cursor.execute(qry)
            res = []
            # field_map = fields(cursor)
            for row in cursor:
                res.append(row)
            print(res)
            if(res):
                 return render_template('index.html', res=res)
            else:
                msg="No records found!!"
                return render_template('index.html', errormsg7=msg)
        else:
            msg="Enter input values"
            return render_template('index.html', errormsg7=msg)

@app.route('/findquakesdayandnight', methods=['GET'])
def findquakesdayandnight():
    if request.method == 'GET':
        dayqry="select count(*) from projectDB..earthquake where SUBSTRING(CAST(time AS VARCHAR(19)), 12, 2)< = 20  and SUBSTRING(CAST(time AS VARCHAR(19)), 12, 2)>=8 and mag>4"
        cursor = cnxn.cursor()
        cursor.execute(dayqry)
        daycount=cursor.fetchone()[0]
        nytqry = "select count(*) from projectDB..earthquake where SUBSTRING(CAST(time AS VARCHAR(19)), 12, 2)> = 20  and SUBSTRING(CAST(time AS VARCHAR(19)), 12, 2)<=8 and mag>4"
        cursor.execute(nytqry)
        nytcount = cursor.fetchone()[0]
        if(daycount > nytcount):
            msg="Large earthquakes more often occur at day"
        else:
            msg = "Large earthquakes more often occur at night"
    return render_template('index.html', msgres=msg)


@app.route('/findquakecommon', methods=['GET'])
def findquakecommon():
    if request.method == 'GET':
        long1 = request.args.get('longitude')
        lat1 = request.args.get('latitude')
        inptdist = request.args.get('distance')
        if(validateInput(long1,"")=='' or validateInput(lat1,"")=='' or validateInput(inptdist,"")==''):
            i=0;
            rows=[]
            qry="select longitude, latitude FROM projectDB..earthquake ";
            cursor.execute(qry)
            rows=cursor.fetchall()
            for row in rows:
               long2=row[0]
               lat2=row[1]
               dist=findDistance(lat1, long1, lat2, long2);
               print("The distance is %.2fkm." % dist)
               if dist < int(inptdist):
                i=i+1
            if(not i==0):
                return render_template('index.html', count=i)
            else:
                msg="No records found!!"
                return render_template('index.html', errormsg2=msg)
        else:
            msg="Enter input values"
            return render_template('index.html', errormsg2=msg)


@app.route('/findnetrows', methods=['GET'])
def findnetrows():
    msg = ""
    net = request.args.get('netvalue')
    magSource='us'
    itr12 = int(request.args.get('itr12'))
    if (validateInput(net, "Net Value") == ''):
        start = int(round(time.time() * 1000))
        for i in range(itr12):
            qry = "select *  FROM projectDB..earthquake WHERE net =? and magSource=? "
            cursor.execute(qry,net,magSource)
            result = cursor.fetchall()
        end = int(round(time.time() * 1000))
        totaltime = end - start
        return render_template('index.html', reslt12=result[0], t12=totaltime)
    else:
        msg = "Enter net value"
        return render_template('index.html', errormsg12=msg)

@app.route('/insertrows', methods=['GET'])
def insertrows():
    msg = ""
    sql = "INSERT INTO projectDB..earthquake (time, latitude,longitude, depth, mag, magType, nst, gap, dmin, rms, net, id, updated, place, horizontalError, depthError, type, magError, magNst, status, locationSource, magSource) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    return render_template('index.html')



@app.route('/upload', methods=['POST'])
def upload():
    msg=''
    starttime = int(round(time.time() * 1000))

    # upload_folder = app.config['UPLOAD_FOLDER']
    # file = request.files['file']
    # file_name = file.filename
    # file.save(os.path.join(upload_folder, file_name))
    upload_file = app.config['UPLOAD_FILE']

    with open("all_month.csv", 'r') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            columns=(row)
            break;
    flag=True
    if flag:
        my_columns = " VARCHAR(250), ".join(columns)
        sql1 = "CREATE TABLE weet (" + my_columns + " VARCHAR(250))"

        sql = """CREATE TABLE sample3(
	[time] [datetime2](7) NOT NULL,
	[latitude] [float] NOT NULL,
	[longitude] [float] NOT NULL,
	[depth] [nvarchar](50) NOT NULL,
	[mag] [float] NOT NULL,
	[magType] [nvarchar](50) NOT NULL,
	[nst] [nvarchar](50) NULL,
	[gap] [nvarchar](50) NULL,
	[dmin] [nvarchar](50) NULL,
	[rms] [nvarchar](50) NOT NULL,
	[net] [nvarchar](50) NOT NULL,
	[id] [nvarchar](50) NOT NULL,
	[updated] [datetime2](7) NOT NULL,
	[place] [nvarchar](100) NOT NULL,
	[type] [nvarchar](50) NOT NULL,
	[horizontalError] [nvarchar](50) NULL,
	[depthError] [nvarchar](50) NULL,
	[magError] [nvarchar](50) NULL,
	[magNst] [nvarchar](50) NULL,
	[status] [nvarchar](50) NOT NULL,
	[locationSource] [nvarchar](50) NOT NULL,
	[magSource] [nvarchar](50) NOT NULL
)"""
        cursor.execute(sql1)
        cursor.commit()

    data = pd.read_csv("./all_month.csv")
    df = pd.DataFrame(data)

    with open("all_month.csv", 'r') as csv_file:
        dataframe = csv.reader('all_month.csv')
    for row in df.itertuples():
        sql = "INSERT INTO weet (" + ','.join(columns) + ")  VALUES (?, ?, ?, ?,?) "
        cursor.execute(sql,row.time,row.latitude,row.longitude,row.depth,row.mag)
        cursor.commit()
        print(row)

    # endtime = int(round(time.time() * 1000))
    # totalexectime = endtime - starttime
    # cursor.commit()
    msg="Inserted Successfully"
    return render_template('index.html',filemsg=msg)




def getclusters():
    if request.method == 'GET':
        from_Mag = request.args.get('from_Mag')
        to_Mag = request.args.get('to_Mag')
        if(validateInput(to_Mag,'')=='' or validateInput(from_Mag,'')==''):
            qry="SELECT * FROM  projectDB..earthquake where mag >= "+from_Mag+" and mag < ="+to_Mag
            cursor.execute(qry)
            #for row in cursor:


def validateInput(inputData,inputField):
    if(not inputData):
        msg="Enter the "+inputField
    else:
        msg=''
    return msg

def convertDate(date):
    dteSplit = date.split("-");
    day = dteSplit[2];
    month = dteSplit[1];
    yr = dteSplit[0];

    convertedDate=yr+"-"+month+"-"+day
    return convertedDate

def findDistance(lat1, long1, lat2, long2):
        R = 6373.0

        lat1 = radians(float(lat1))
        long1 = radians(float(long1))
        lat2 = radians(float(lat2))
        long2 = radians(float(long2))

        dlon = long2 - long1
        dlat = lat2 - lat1

        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        distance = R * c
        return distance

def fields(cursor):
    results = {}
    column = 0
    for d in cursor.description:
        results[d[0]] = column
        column = column + 1
    return results



if __name__ == "__main__":
    app.run(host="0.0.0.0")
