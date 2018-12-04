import os
import datetime

from pyspark import SQLContext, SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf

sc = SparkContext()
sqlContext = SQLContext(sc)

dataPath = 'file://' + os.path.abspath('data/Police_Department_Incident_Reports__Historical_2003_to_May_2018.csv')

crimeDataSchema = StructType([StructField("IncidntNum", LongType(), True),
                              StructField("Category", StringType(), True),
                              StructField("Descript", StringType(), True),
                              StructField("DayOfWeek", StringType(), True),
                              StructField("Date", StringType(), True),
                              StructField("Time", StringType(), True),
                              StructField("PdDistrict", StringType(), True),
                              StructField("Resolution", StringType(), True),
                              StructField("Address", StringType(), True),
                              StructField("X", DoubleType(), True),
                              StructField("Y", DoubleType(), True),
                              StructField("Location", StringType(), True),
                              StructField("PdId", LongType(), True)])

crimeDF = (sqlContext.read
           .format('csv')
           .option('delimiter', ',')
           .option('header', 'true')
           .load(dataPath, schema=crimeDataSchema))


def parseDate(dateStr):
    tokens = dateStr.split("/")
    month = int(tokens[0])
    date = int(tokens[1])
    year = int(tokens[2])
    return datetime.date(year, month, date)


def parseTime(timeStr):
    tokens = timeStr.split(":")
    hour = int(tokens[0])
    minute = int(tokens[1])
    return datetime.datetime(year=1, month=1, day=1, hour=hour, minute=minute)


crimeDF = (crimeDF.withColumn("Date_tmp", udf(parseDate, DateType())(crimeDF.Date))
           .withColumn("Time_tmp", udf(parseTime, TimestampType())(crimeDF.Time))
           .withColumnRenamed("Time", "TimeStr")
           .withColumnRenamed("Date", "DateStr")
           .withColumnRenamed("Date_tmp", "Date")
           .withColumnRenamed("Time_tmp", "Time")).cache()

categories = crimeDF.select("Category").distinct().collect()
categories = list(map(lambda e: e[0], categories))
crimeDF.printSchema()


def filterByDate(df, startDate, endDate):
    return df.filter(df.Date > startDate).filter(df.Date < endDate)


def filterByTime(df, startTime, endTime):
    return df.filter(df.Time > startTime).filter(df.Time < endTime)


def filterByCategory(df, category):
    return df.filter(df.Category == category)


def getFilteredPoints(startDate, endDate, startTime, endTime, category):
    filteredDF = filterByDate(crimeDF, startDate, endDate)
    filteredDF = filterByTime(filteredDF, startTime, endTime)

    if not category == 'ALL':
        filteredDF = filterByCategory(filteredDF, category)

    pointsDF = filteredDF.select("X", "Y").rdd.map(lambda row: {"COORDINATE": [row["X"], row["Y"]]})
    return pointsDF.collect()


def getFilteredDistricts(startDate, endDate, startTime, endTime, category):
    filteredDF = filterByDate(crimeDF, startDate, endDate)
    filteredDF = filterByTime(filteredDF, startTime, endTime)

    if not category == 'ALL':
        filteredDF = filterByCategory(filteredDF, category)

    sqlContext.sql("DROP TABLE IF EXISTS crime_dataset")
    sqlContext.registerDataFrameAsTable(filteredDF, "crime_dataset")
    districtsDF = (sqlContext.sql(
        "select PdDistrict as d, avg(X) as x, avg(Y) as y, count(*) as o from crime_dataset group by PdDistrict"))

    return districtsDF.rdd.map(lambda r: (r["d"], r["x"], r["y"], r["o"])).collect()


import flask
import json
from flask_cors import CORS
from flask import request

app = flask.Flask(__name__)
CORS(app)


@app.route('/data', methods=['GET'])
def handleData():
    cat = request.args.get(key='cat', default='ALL')
    startD = request.args.get(key='startDate', default='1/1/2018')
    endD = request.args.get(key='endDate', default='2/2/2018')
    startT = request.args.get(key='startTime', default='0')
    endT = request.args.get(key='endTime', default='23')

    print(startD, endD)

    startDate = parseDate(startD)
    endDate = parseDate(endD)

    print(startDate, endDate)

    startTime = datetime.datetime(year=1, month=1, day=1, hour=int(startT), minute=0)
    endTime = datetime.datetime(year=1, month=1, day=1, hour=int(endT), minute=59)

    crimePoints = getFilteredPoints(startDate, endDate, startTime, endTime, cat)
    return json.dumps(crimePoints)


@app.route('/categories')
def handleCategory():
    return json.dumps(categories)


if __name__ == '__main__':
    app.run()
