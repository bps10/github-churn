from flask import Flask
app = Flask(__name__)
from flaskexample import views
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel

spark = SparkSession.builder.appName('App').getOrCreate()
testM = LogisticRegressionModel.load("lrModel")
print('-----Model loaded-----')

