from flaskexample import app
import pandas as pd
import numpy as np
import matplotlib.pylab as plt
from flask import render_template, request

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import SparseVector
from pyspark.sql import functions as F

from github3 import login
#from lifetimes import BetaGeoFitter
import helper as h

from google.cloud import bigquery
client = bigquery.Client()

# GitHub api
password = h.get_gh_credentials()
gh = login('bps10', password=password)

# Load PySpark pipeline and model
spark = SparkSession.builder.appName('App').getOrCreate()

LRmodelCompany = LogisticRegressionModel.load("lrModel_company_1")
LRmodelLow = LogisticRegressionModel.load("lrModel_company_0high_low_0")
LRmodelHigh = LogisticRegressionModel.load("lrModel_company_0high_low_1")
KMmodel = KMeansModel.load("KMeans_model")

print('-----Models loaded-----')
pipeline = Pipeline.load('pipeline')
print('-----Pipeline loaded-----')
 
pop_repos = pd.read_pickle('popular_repos.pickle')
pop_repos['last_event'] = pop_repos.last_event.astype(str)
pop_repos['last_event'] = pop_repos.last_event.fillna('2018-08-01 01:00:00+00:00')
#pop_repos = pop_repos.drop(columns=['bio'])
#pop_repos = h.process_raw_df(pop_repos)
pop_repos = pop_repos.drop(columns=['actor_login'])
print(pop_repos)
# Load CVL model
#CLV_model = BetaGeoFitter()
#CLV_model.load_model('CLV.pkl')

@app.route('/repo', methods=("POST", "GET"))
def html_table():
	reponame = 'matplotlib'
	contributors = pop_repos[pop_repos.repo == reponame]		
	#contributors = contributors.iloc[[0]]
	print(contributors.last_event)
	contributors['frequency'] = contributors['event_count']	
	contributors['second_period_event_count'] = np.zeros(len(contributors))

	contributors = pandas_to_spark(contributors, False)
	contributors = h.create_KMeans_features(contributors)	
	contributors = KMmodel.transform(contributors)		
	print(contributors)

	company_users = contributors[contributors.company == 1]
	high_users = contributors[contributors.high_low_user == 1]
	low_users = contributors[contributors.high_low_user == 0]

	all_data = []
	all_data = predict_users(company_users, LRmodelCompany, all_data)
	all_data = predict_users(high_users, LRmodelHigh, all_data)
	all_data = predict_users(low_users, LRmodelLow, all_data)

	print(all_data)
	# sort the data based on probability	
	# compute a histogram
	all_data = pd.concat(all_data)

	all_data = all_data.sort_values(by='probability', ascending=True)

	#fig, ax = plt.subplots(1, 1)
	#all_data.probability.hist(ax=ax, bins=10)
	#fig.savefig('/flaskexample/static/probability.png')

	#event_data = h.get_user_events('bps10', bigquery, client)
	return render_template('table.html', 
		tables=[all_data.to_html(classes='data')], 
		titles=all_data.columns.values,
		#url ='/flaskexample/static/probability.png'
		)


@app.route('/')
def index():
	return render_template('index.html', name='Brian')

@app.route('/', methods=['POST'])
def data_page():
	username = request.form['username']

	user_profile = h.get_user_info(gh, username)
	user_profile_df = pd.DataFrame(user_profile, index=[0])
	user_profile_df = user_profile_df.fillna(0)

	event_data = h.get_user_events(username, bigquery, client)
	event_data['last_event'] = event_data.last_event.fillna(pd.to_datetime('2018-08-01 01:00:00'))
	event_data = event_data.fillna(0)
	#event_data = add_time_fields(event_data, user_profile_df)

	usr_data = user_profile_df.join(event_data)
	usr_data['second_period_event_count'] = [0]	
	usr_data = h.process_raw_df(usr_data)
	print(usr_data.transpose())	
		
	spark_user = pandas_to_spark(usr_data)

	model = get_user_specific_model(spark_user)
	predictions = get_predict_dict(spark_user, model)

	#print(CLV_model.fit([19], [50], [100]))

	return render_template('index.html',
							stay_or_go=predictions['stay_or_go0'],
							name=username,
							followers_count=user_profile['followers_count'],
							following_count=user_profile['following_count'],
							probability=predictions['probability0'],
							addUserProb=predictions['probability1'],
							addWatchProb=predictions['probability2'],
							addBlog=predictions['probability3'],
							addAllThree=predictions['probability4'])

# -------------------------------------------------
# -------------------------------------------------
def predict_users(data, model, all_data):	
	# run the model		
	
	data = data.withColumn("second_period_event_count", 
		data.second_period_event_count.cast(DoubleType())
		)
	pipelineModel = pipeline.fit(data)
	usr_data = pipelineModel.transform(data)

	prediction = model.transform(usr_data)
	prediction = prediction.select(['login', 'probability',
									'prediction']).toPandas()
	
	prediction['probability'] = prediction.probability.apply(lambda x: x[1])

	all_data += [prediction]
	return all_data


def get_user_specific_model(spark_user, userIndex=0):
	spark_user = h.create_KMeans_features(spark_user)
	spark_user = KMmodel.transform(spark_user)
	print('---------------------')
	spark_df = spark_user.toPandas()
	print(spark_df)
	if spark_df.iloc[userIndex].company:
		print('model = LRmodelCompany')
		model = LRmodelCompany
	else:
		if spark_df.iloc[userIndex].high_low_user:
			print('model = LRmodelHigh')
			model = LRmodelHigh
		else:
			print('model = LRmodelLow')
			model = LRmodelLow

	return model


def pandas_to_spark(usr_data, drop_bio=True):
	#if drop_bio:
	#	usr_data = usr_data.drop(columns=['bio'])
	print('----------here---------')
	print(usr_data.bio) 
	spark_user = spark.createDataFrame(usr_data)		
	print('----------here2---------')	
	spark_user = h.convert_bigint_to_int(spark_user)
	spark_user = h.add_date_info_spark(spark_user, convert=False)
	spark_user = spark_user.withColumn('recency', F.log(spark_user.recency + 1))
	#print(spark_user.head())
	return spark_user


def get_predict_dict(spark_user, model):
	print(spark_user)
	print('=======Predicting 1 =========')	
	p = {}
	p['stay_or_go0'], p['probability0'] = predict(spark_user, model)	
	
	# add one follower and rerun model
	print('=======Predicting 2 =========')		
	new_data = spark_user.withColumn(
	'followers_count', spark_user['followers_count'] + 1)
	p['stay_or_go1'], p['probability1'] = predict(new_data, model)	

	# add one follower and rerun model
	print('=======Predicting 3 =========')
	new_data = spark_user.withColumn(
	'WatchEvent_count', spark_user['WatchEvent_count'] + 1)
	p['stay_or_go2'], p['probability2'] = predict(new_data, model)	

	# add one follower and rerun model
	print('=======Predicting 4 =========')
	new_data = spark_user.withColumn(
	'blog', spark_user['blog'] + 1)
	p['stay_or_go3'], p['probability3'] = predict(new_data, model)	

	new_data = spark_user.withColumn(
	'blog', spark_user['blog'] + 1)
	new_data = new_data.withColumn(
	'WatchEvent_count', new_data['WatchEvent_count'] + 1)
	new_data = new_data.withColumn(
	'followers_count', new_data['followers_count'] + 1)

	p['stay_or_go4'], p['probability4'] = predict(new_data, model)
	print(p)

	return p

def predict(data, model):
	data = data.withColumn("second_period_event_count", 
		data.second_period_event_count.cast(DoubleType())
		)
	pipelineModel = pipeline.fit(data)
	usr_data = pipelineModel.transform(data)

	prediction = model.transform(usr_data)
	prediction = prediction.select(['probability',
									'prediction']).collect()#[0][1]
	if prediction[0][1] > 0.5:
		stay_or_go = 'remain active'
		probability = str(round(prediction[0][0][1], 2))
	else:
		stay_or_go = 'leave GitHub'
		probability = str(round(prediction[0][0][0], 2))
	return stay_or_go, probability


def add_time_fields(event_data, user_df):
	print(event_data.last_event)
	print(event_data)
	event_data['time_between_first_last_event'] = (
		pd.to_datetime(event_data.last_event) -
		pd.to_datetime(event_data.first_event)) / pd.Timedelta(days=1)
	event_data['recency'] = (
		pd.to_datetime(event_data.last_event[:-4]) -
		pd.to_datetime(user_df.created_at[:-4])) / pd.Timedelta(days=1)

	return event_data
