from flaskexample import app
import pandas as pd
import numpy as np
from flask import render_template, request
import json

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import SparseVector
from pyspark.sql import functions as F

from github3 import login
from lifetimes import BetaGeoFitter
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
pop_repos['first_event'] = pop_repos.first_event.astype(str)
pop_repos = h.pandas_fill_na(pop_repos, last_event_fill='2018-08-01 01:00:00')
print(pop_repos[['first_event', 'last_event']])

pop_repos = pop_repos.drop(columns=['actor_login'])
repos = pop_repos.repo.unique()

# Load CVL model
CLV_model = BetaGeoFitter()
CLV_model.load_model('CLV.pkl')

#@app.route('/repo', methods=("POST", "GET"))
def html_table(reponame):
	#reponame = 'matplotlib'
	contributors = pop_repos[pop_repos.repo == reponame]
	print('++++ {0} contributors to {1} ++++'.format(len(contributors), reponame))
	
	contributors['second_period_event_count'] = np.zeros(len(contributors))
	contributors = pandas_to_spark(contributors)	

	if reponame == 'matplotlib':
		print(pop_repos[pop_repos.login == 'meeseeksmachine'].transpose())

	company_users = contributors[contributors.company == 1]
	high_users = contributors[contributors.high_low_user == 1]
	low_users = contributors[contributors.high_low_user == 0]

	all_data = []
	all_data = predict_users(company_users, LRmodelCompany, all_data)
	all_data = predict_users(high_users, LRmodelHigh, all_data)
	all_data = predict_users(low_users, LRmodelLow, all_data)

	print(all_data)
	# sort the data based on probability	
	all_data = pd.concat(all_data)

	all_data = all_data.sort_values(by='probability', ascending=True)
	all_data = all_data.round({'probability': 4}).set_index('login')

	return all_data

@app.route('/')
def index():
	return render_template('index.html', 
		username_exists=False,
		repos=repos,
		name=False)

@app.route('/', methods=['POST'])
def data_page():	
	username_exists = False
	if 'username' in request.form:
		username = request.form['username']
		username_exists = True
	try:		
		user_profile = h.get_user_info(gh, username)		
	except:
		username_exists = False		

	reponame_exists = False
	try:		
		reponame = request.form.get('reponame')
		reponame_exists = True
		print(reponame)
	except:
		reponame_exists = False

	if username_exists:
		user_profile_df = pd.DataFrame(user_profile, index=[0])
		user_profile_df = user_profile_df.fillna(0)

		event_data = h.get_user_events(username, bigquery, client)				

		usr_data = user_profile_df.join(event_data)
		usr_data['second_period_event_count'] = [0]	
		usr_data = h.pandas_fill_na(usr_data, last_event_fill='2018-08-01 01:00:00')
		print(usr_data.transpose())	
			
		spark_user = pandas_to_spark(usr_data)
		model = get_user_specific_model(spark_user)
		predictions = get_predict_suggestions(spark_user, model)

		#print(CLV_model.fit([19], [50], [100]))

		return render_template('index.html',
								user_profile=user_profile,
								username_exists=username_exists,
								prediction=predictions,								
								name=username,
								repos=repos,
								)

	elif reponame_exists:

		all_data = html_table(reponame)		
		hist_vals, edges = np.histogram(all_data.probability, bins=10, range=[0, 1])
		labels = np.round(np.arange(0.1, 1.1, 0.1), 1)
		all_data = all_data[:25]

		return render_template('index.html', 
				reponame_exists=reponame_exists,
				tables=[all_data.to_html(classes='table table-striped')], 
				titles=all_data.columns.values,
				url ='static/probability.png',
				repos=repos,
				reponame=reponame,
				labels=labels, 
				values=hist_vals
				)
		
# -------------------------------------------------
# -------------------------------------------------


def predict_single_user(data, model):
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
	spark_df = spark_user.toPandas()	
	print('-------------IN get_user_specific_model --------')
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


def pandas_to_spark(usr_data):	
	print(usr_data.bio.transpose()) 	
	spark_user = spark.createDataFrame(usr_data)			
	spark_user = h.convert_bigint_to_int(spark_user)
	spark_user = h.add_date_info_spark(spark_user, convert=False)	
	spark_user = h.feature_scaling(spark_user)
	spark_user = h.create_KMeans_features(spark_user)
	spark_user = KMmodel.transform(spark_user)	
	
	return spark_user


def get_predict_suggestions(spark_user, model):
	print(spark_user)
	print('=======Predicting 1 =========')	
	p = {}
	p['stay_or_go0'], p['probability0'] = predict_single_user(spark_user, model)	
	
	# add one follower and rerun model
	print('=======Predicting 2 =========')		
	new_data = spark_user.withColumn(
	'followers_count', spark_user['followers_count'] + 1)
	p['stay_or_go1'], p['probability1'] = predict_single_user(new_data, model)	

	# add one follower and rerun model
	print('=======Predicting 3 =========')
	new_data = spark_user.withColumn(
	'WatchEvent_count', spark_user['WatchEvent_count'] + 1)
	p['stay_or_go2'], p['probability2'] = predict_single_user(new_data, model)	

	# add one follower and rerun model
	print('=======Predicting 4 =========')
	new_data = spark_user.withColumn(
	'blog', spark_user['blog'] + 1)
	p['stay_or_go3'], p['probability3'] = predict_single_user(new_data, model)	

	new_data = spark_user.withColumn(
	'blog', spark_user['blog'] + 1)
	new_data = new_data.withColumn(
	'WatchEvent_count', new_data['WatchEvent_count'] + 1)
	new_data = new_data.withColumn(
	'followers_count', new_data['followers_count'] + 1)

	p['stay_or_go4'], p['probability4'] = predict_single_user(new_data, model)
	print(p)

	return p

'''
def add_time_fields(event_data, user_df):
	print(event_data.last_event)
	print(event_data)
	event_data['time_between_first_last_event'] = (
		pd.to_datetime(event_data.last_event) -
		pd.to_datetime(event_data.first_event)) / pd.Timedelta(days=1)
	event_data['recency'] = (
		pd.to_datetime(event_data.last_event[:-4]) -
		pd.to_datetime(user_df.created_at[:-4])) / pd.Timedelta(days=1)

	return event_data'''
