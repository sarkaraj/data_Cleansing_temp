from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("test").setMaster("local[*]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
sc.setLogLevel("ERROR")

data = sc.textFile("./data/love_travel_data")

data.take(5)

