import pandas as pd
import numpy as np
import matplotlib.pylab as plt
import pandas as pd
import os

from pyspark.sql import SparkSession, udf
from pyspark.sql.functions import to_timestamp, datediff
from pyspark.sql.types import IntegerType, FloatType, DoubleType, BooleanType


## feature scaling
def feature_scaling(df):
    pass


def get_gh_credentials():
    f = open('gh-password.txt', 'r') 
    return f.read()[:-1]


def eval_metrics(predictions):
    predictions = predictions.toPandas()
    TP = np.sum((predictions.label == 1) & (predictions.prediction == 1))
    FP = np.sum((predictions.label == 0) & (predictions.prediction == 1))
    FN = np.sum((predictions.label == 1) & (predictions.prediction == 0))
    TN = np.sum((predictions.label == 0) & (predictions.prediction == 0))
    precision = TP / (TP + FP)
    recall = TP / (TP + FN)
    accuracy = (TP + TN) / (TP + FP + FN + TN)
    f1score = 2 * (precision * recall) /  (precision + recall)
    print('Precision: {0}'.format(np.round(precision, 3)))
    print('Recall:    {0}'.format(np.round(recall, 3)))
    print('Accuracy:  {0}'.format(np.round(accuracy, 3)))
    print('F1-score:  {0}'.format(np.round(f1score, 4)))


def add_time_columns(df, end_date='2016-06-01 23:59:59+00:00'):
    
    df['created_at'] = pd.to_datetime(df.created_at, errors='coerce')
    df['last_event'] = pd.to_datetime(df.last_event, errors='coerce')
    df['first_event'] = pd.to_datetime(df.first_event, errors='coerce')
    
    end_date = pd.to_datetime(end_date) 
    df['T'] = np.round((end_date - df.created_at) / pd.Timedelta(weeks = 1))
    df['recency'] = (df.last_event - df.created_at)  / pd.Timedelta(weeks=1)
    df['time_between_first_last_event'] = (df.last_event - df.first_event) / pd.Timedelta(days=1)
    return df


def write_tree_to_file(tree, filename):
    fullfile = os.path.join("trees", filename + ".txt")
    text_file = open(fullfile, "w")
    text_file.write(tree)
    text_file.close()
    print('Saved to fullfile')

    
def get_merged_data(appName='gh-churn', year='2016'):
    
    spark = SparkSession.builder.appName(appName).getOrCreate()
    first_period = spark.read.csv('events_data/events_' + year + '_01_01_' + year + '_06_01.csv', 
                                  header = True, inferSchema = True)    
    second_period = spark.read.csv('events_data/events_' + year + '_06_02_' + year + '_11_01.csv', 
                                   header = True, inferSchema = True)
    users = spark.read.csv('user_data/all_users.csv', header=True, inferSchema=True)
    users = users.drop('event_count').drop('last_event').drop('first_event').drop('_c0')
    
    churn_data = users.join(first_period, users['login'] == first_period['actor'], 
                            how='left')

    keep_columns =  ['CommitCommentEvent_count', 'CreateEvent_count', 'DeleteEvent_count', 
                   'ForkEvent_count', 'GollumEvent_count', 'IssueCommentEvent_count',
                   'IssuesEvent_count', 'MemberEvent_count', 'PublicEvent_count', 
                   'PullRequestEvent_count', 'PullRequestReviewCommentEvent_count',
                   'PushEvent_count', 'ReleaseEvent_count', 'WatchEvent_count']
    
    second_period = second_period.withColumn('event_count', 
                             sum(second_period[col] for col in second_period.columns if col in keep_columns))
    
    churn_data = churn_data.withColumn('frequency', 
                             sum(churn_data[col] for col in churn_data.columns if col in keep_columns))
    
    second_period_event_count = second_period.selectExpr(
        "actor as login", "event_count as second_period_event_count")
    
    churn_data = churn_data.join(second_period_event_count,
                                 on='login', how='left')
    churn_data = churn_data.fillna(0, subset='second_period_event_count')

    '''
    churn_data = churn_data.withColumn("first_event", to_timestamp(churn_data.first_event))
    churn_data = churn_data.withColumn("last_event", to_timestamp(churn_data.last_event))
    churn_data = churn_data.withColumn("created_at", to_timestamp(churn_data.created_at))
    churn_data = churn_data.withColumn("updated_at", to_timestamp(churn_data.updated_at))
    
    churn_data = churn_data.withColumn("time_between_first_last_event", 
                                   datediff(churn_data.last_event, churn_data.first_event))
    '''
    
    churn_data = churn_data.withColumn("public_repos_count",
                                       churn_data.public_repos_count.cast(IntegerType()))
    churn_data = churn_data.withColumn("public_gists_count", 
                                       churn_data.public_gists_count.cast(IntegerType()))
    churn_data = churn_data.withColumn("followers_count", 
                                       churn_data.followers_count.cast(IntegerType()))
    churn_data = churn_data.withColumn("following_count", 
                                       churn_data.following_count.cast(IntegerType()))
    
    f_udf=udf.UserDefinedFunction(lambda x: 1 if x is not None else 0, IntegerType())

    churn_data = churn_data.withColumn("blog", f_udf(churn_data.blog))
    churn_data = churn_data.withColumn("company", f_udf(churn_data.company))
    churn_data = churn_data.withColumn("hireable", f_udf(churn_data.hireable))
    #churn_data = churn_data.withColumnRenamed("event_count", "frequency")

    churn_data = churn_data.select('login', 'followers_count', 'following_count', 'blog',
                   'company', 'created_at', 'public_repos_count', 'public_gists_count',
                   'hireable',
                   'updated_at', 'time_between_first_last_event', 'last_event', 'first_event',
                   'frequency', 
                   'second_period_event_count',
                   'CommitCommentEvent_count', 'CreateEvent_count', 'DeleteEvent_count', 
                   'ForkEvent_count', 'GollumEvent_count', 'IssueCommentEvent_count',
                   'IssuesEvent_count', 'MemberEvent_count', 'PublicEvent_count', 
                   'PullRequestEvent_count', 'PullRequestReviewCommentEvent_count',
                   'PushEvent_count', 'ReleaseEvent_count', 'WatchEvent_count')
    
    # remove outliers with very high number of Events.
    n_event_threshold = 200 if year == '2016' else 600
    n_total_users = churn_data.count()
    churn_data = churn_data.filter(churn_data.frequency < n_event_threshold)
    churn_data = churn_data.filter(churn_data.second_period_event_count < n_event_threshold)
    n_users = churn_data.count()    
    print('% of users dropped {0}'.format(100 - (n_users / n_total_users * 100)))
    
    return churn_data


def get_user_info(gh, user):
    user_info = {'login': user}
    this_user = gh.user(user)
    user_info['followers_count'] = this_user.followers_count
    user_info['following_count'] = this_user.following_count
    user_info['bio'] = this_user.bio
    user_info['blog'] = this_user.blog
    user_info['company'] = this_user.company
    user_info['created_at'] = this_user.created_at
    user_info['public_repos_count'] = this_user.public_repos_count
    user_info['public_gists_count'] = this_user.public_gists_count
    user_info['hireable'] = this_user.hireable
    user_info['updated_at'] = this_user.updated_at

    return user_info

def get_batch(gh, df, random_indexes, start_index, existing_users=set(),
              batch_size=5000):
    '''
    '''
    d = {}
    i = start_index
    count = 0
    while count < batch_size:
        if count % 200 == 0:
            print(count)

        user = df.iloc[random_indexes[i]]
        if user.actor in existing_users:
            i += 1
        else:
            try:
                user_info = get_user_info(gh, user.actor)
                d[user.actor] = user_info
                d[user.actor]['event_count'] = user.event_count
                d[user.actor]['last_event'] = user.last_event
                d[user.actor]['first_event'] = user.first_event
                d[user.actor]['time_between_first_last_event'] = user.time_between_first_last_event
                
                count += 1
                i += 1
            except:
                # expected to fail if user has deleted profile, counts against rate limited api calls.
                count += 1
                i += 1
                print(user.actor)
            
    return d, start_index + batch_size



def get_user_events(user, bigquery, client):
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = True

    query = (
    """SELECT  
      COUNT(type) as event_count, 
      MAX(created_at) as last_event, 
      MIN(created_at) as first_event,
      SUM(CASE WHEN type = 'CommitCommentEvent' then 1 else 0 end) as CommitCommentEvent_count,
      SUM(CASE WHEN type = 'CreateEvent' then 1 else 0 end) as CreateEvent_count,
      SUM(CASE WHEN type = 'DeleteEvent' then 1 else 0 end) as DeleteEvent_count,
      SUM(CASE WHEN type = 'ForkEvent' then 1 else 0 end) as ForkEvent_count,
      SUM(CASE WHEN type = 'GollumEvent' then 1 else 0 end) as GollumEvent_count,
      SUM(CASE WHEN type = 'IssueCommentEvent' then 1 else 0 end) as IssueCommentEvent_count,
      SUM(CASE WHEN type = 'IssuesEvent' then 1 else 0 end) as IssuesEvent_count,
      SUM(CASE WHEN type = 'MemberEvent' then 1 else 0 end) as MemberEvent_count,
      SUM(CASE WHEN type = 'PublicEvent' then 1 else 0 end) as PublicEvent_count,
      SUM(CASE WHEN type = 'PullRequestEvent' then 1 else 0 end) as PullRequestEvent_count,
      SUM(CASE WHEN type = 'PullRequestReviewCommentEvent' then 1 else 0 end) as PullRequestReviewCommentEvent_count,
      SUM(CASE WHEN type = 'PushEvent' then 1 else 0 end) as PushEvent_count,
      SUM(CASE WHEN type = 'ReleaseEvent' then 1 else 0 end) as ReleaseEvent_count,
      SUM(CASE WHEN type = 'WatchEvent' then 1 else 0 end) as WatchEvent_count,

      FROM 
      [githubarchive:year.2018]
    WHERE (actor.login = '""" + user + """' AND public);

    """

    )
    df = client.query(query, location="US", job_config=job_config).to_dataframe()
    
    return df


def convert_bigint_to_int(df):
    for col, t in df.dtypes:
        if t == 'bigint':
            df = df.withColumn(col, df[col].cast(IntegerType()))
            
    f_udf=udf.UserDefinedFunction(lambda x: 1 if x is not None else 0, IntegerType())
    df = df.withColumn("blog", f_udf(df.blog))
    df = df.withColumn("company", f_udf(df.company))
    df = df.withColumn("hireable", f_udf(df.hireable))
    
    return df


if __name__ == '__main__':
    df = get_merged_data()
    print(df.head())