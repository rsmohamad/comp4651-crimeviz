import os
from pyspark import SQLContext, SparkContext
from pyspark.sql.types import *

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
           .format('com.databricks.spark.csv')
           .option('delimiter', ',')
           .option('header', 'true')
           .load(dataPath, schema=crimeDataSchema))

# Register dataframe as sql table
sqlContext.sql("DROP TABLE IF EXISTS crime_dataset")
sqlContext.registerDataFrameAsTable(crimeDF, "crime_dataset")

# lat = sqlContext.sql("select Y from crime_dataset").rdd.map(lambda row: (row["Y"])).take(500)
# lon = sqlContext.sql("select X from crime_dataset").rdd.map(lambda row: (row["X"])).take(500)

crimePoints = (sqlContext.sql("select X, Y from crime_dataset")
               .rdd.map(lambda row: {"COORDINATE": [row["X"], row["Y"]]})).take(15000)

categories = (sqlContext.sql("select distinct Category from crime_dataset").rdd.map(lambda row: row["Category"])).collect()

import flask
import json
from flask_cors import CORS

app = flask.Flask(__name__)
CORS(app)


@app.route('/data', methods=['GET'])
def handleData():
    return json.dumps(crimePoints)


@app.route('/categories')
def handleCategory():
    return json.dumps(categories)


if __name__ == '__main__':
    app.run()