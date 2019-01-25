from flaskexample import app
import pandas as pd
from flask import render_template, request

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import SparseVector

from github3 import login
from lifetimes import BetaGeoFitter
import json

import helper as h

from google.cloud import bigquery
client = bigquery.Client()

# GitHub api
password = h.get_gh_credentials()
gh = login('bps10', password=password)

# Load PySpark pipeline and model
spark = SparkSession.builder.appName('App').getOrCreate()
testM = LogisticRegressionModel.load("lrModel")
print('-----Model loaded-----')
pipeline = Pipeline.load('pipeline')
print('-----Pipeline loaded-----')

d = {'login': '0loky0',
     'followers_count': 0,
     'following_count': 1,
     'blog': 0,
     'company': 0,
     'created_at': '2011-05-24 20:15:25+00:00',
     'public_repos_count': 7,
     'public_gists_count': 0,
     'hireable': 1,
     'updated_at': '2019-01-09 15:03:59+00:00',
     'time_between_first_last_event': '10 days 15:29:06.000000000',
     'last_event': '2016-04-15 10:14:03 UTC',
     'first_event': '2016-04-04 18:44:57 UTC',
     'frequency': 19,
     'second_period_event_count': 0,
     'WatchEvent_count': 5,
     'CommitCommentEvent_count': 0,
     'CreateEvent_count': 5,
     'DeleteEvent_count': 0,
     'ForkEvent_count': 2,
     'GollumEvent_count': 0,
     'IssueCommentEvent_count': 0,
     'IssuesEvent_count': 0,
     'MemberEvent_count': 0,
     'PublicEvent_count': 0,
     'PullRequestEvent_count': 0,
     'PullRequestReviewCommentEvent_count': 0,
     'PushEvent_count': 12,
     'ReleaseEvent_count': 0
     }

with open('data.json', 'w') as fp:
    json.dump(d, fp)

# Load CVL model
CLV_model = BetaGeoFitter()
CLV_model.load_model('CLV.pkl')

@app.route('/')
def index():
  return render_template('index.html', name='Brian')

@app.route('/', methods=['POST'])
def data_page():
  username = request.form['username']

  user_profile = h.get_user_info(gh, username)
  user_profile_df = pd.DataFrame(user_profile, index=[0])

  event_data = h.get_user_events(username, bigquery, client)
  usr_data = user_profile_df.join(event_data)
  usr_data['second_period_event_count'] = [0]
  usr_data = usr_data.fillna(0)
  print(usr_data)
  print(usr_data.transpose())

  spark_user = spark.createDataFrame(usr_data)
  spark_user = spark_user.drop('bio')
  spark_user = h.convert_bigint_to_int(spark_user)

  #data = spark.read.json('data.json')
  stay_or_go, probability = predict(spark_user)
  print([username, stay_or_go, probability])

  # add one follower and rerun model
  new_data = spark_user.withColumn(
    'followers_count', spark_user['followers_count'] + 1)
  stay_or_go1, probability1 = predict(new_data)

  # add one follower and rerun model
  new_data = spark_user.withColumn(
    'WatchEvent_count', spark_user['WatchEvent_count'] + 1)
  stay_or_go2, probability2 = predict(new_data)
  print(probability2)

  # add one follower and rerun model
  new_data = spark_user.withColumn(
    'blog', spark_user['blog'] + 1)
  stay_or_go3, probability3 = predict(new_data)
  print(probability3)

  new_data = spark_user.withColumn(
    'blog', spark_user['blog'] + 1)
  new_data = new_data.withColumn(
    'WatchEvent_count', new_data['WatchEvent_count'] + 1)
  new_data = new_data.withColumn(
    'followers_count', new_data['followers_count'] + 1)

  stay_or_go4, probability4 = predict(new_data)
  print('All three {0}'.format(probability4))

  print(CLV_model.fit([19], [50], [100]))

  return render_template('index.html',
                         stay_or_go=stay_or_go,
                         name=username,
                         followers_count=user_profile['followers_count'],
                         following_count=user_profile['following_count'],
                         probability=probability,
                         addUserProb=probability1,
                         addWatchProb=probability2,
                         addBlog=probability3,
                         addAllThree=probability4)


def predict(data):
  data = data.withColumn("second_period_event_count",                                   data.second_period_event_count.cast(DoubleType()))
  print(data.head())
  pipelineModel = pipeline.fit(data)
  print(pipelineModel)
  usr_data = pipelineModel.transform(data)

  prediction = testM.transform(usr_data)
  prediction = prediction.select(['probability',
                                  'prediction']).collect()#[0][1]
  if prediction[0][1] > 0.5:
    stay_or_go = 'remain active'
    probability = str(round(prediction[0][0][1], 2))
  else:
    stay_or_go = 'leave GitHub'
    probability = str(round(prediction[0][0][0], 2))
  return stay_or_go, probability
