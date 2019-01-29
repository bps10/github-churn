import pandas as pd
import numpy as np
import matplotlib.pylab as plt
import pandas as pd
import os

from pyspark.sql import SparkSession, udf, DataFrame
from pyspark.sql.functions import to_timestamp, datediff
from pyspark.sql.types import IntegerType, FloatType, DoubleType, BooleanType, StringType
from pyspark.sql import functions as F
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

from google.cloud import bigquery


count_columns =  ['CommitCommentEvent_count', 'CreateEvent_count', 'DeleteEvent_count',
                  'ForkEvent_count', 'GollumEvent_count', 'IssueCommentEvent_count',
                  'IssuesEvent_count', 'MemberEvent_count', 'PublicEvent_count', 
                  'PullRequestEvent_count', 'PullRequestReviewCommentEvent_count',
                  'PushEvent_count', 'ReleaseEvent_count', 'WatchEvent_count']
    
scale_columns = ['followers_count', 'following_count',
                'public_repos_count', 'public_gists_count']    


def create_KMeans_features(df):
    df = df.withColumn('non_passive_events',
                         F.log(df.frequency -
                               (df.DeleteEvent_count + 
                                df.GollumEvent_count +
                                df.IssueCommentEvent_count +
                                df.MemberEvent_count +
                                df.WatchEvent_count) + 1)
                        )

    df = df.withColumn('public_repos_gists',
        F.log(df.public_repos_count + df.public_gists_count) + 1)

    # Assemble pipeline
    stages = [VectorAssembler(inputCols=['non_passive_events', 'public_repos_gists'], 
                                outputCol="KMeans_features").setHandleInvalid("skip")]

    pipeline = Pipeline(stages = stages)
    pipelineModel = pipeline.fit(df)
    df = pipelineModel.transform(df)
    #selectedCols = ['label', 'features']
    #churn_data = churn_data.select(selectedCols)
    #churn_data.printSchema()
    return df

def process_raw_df(df):
    # 1. Handle NaN
    df[count_columns] = df[count_columns].fillna(0)
    df[scale_columns] = df[scale_columns].fillna(0)
    # 2. Feature scaling.
    df = feature_scaling(df)
    # 3. Rename event_data
    df = df.rename(index=str, columns={"event_count": "frequency"})

    return df


def print_user_churn(df):
    if isinstance(df, DataFrame):
        df = df.toPandas()
        
    print('{0}% of users churned in second period'.format(
        np.round(np.sum(df.second_period_event_count < 1) / len(df) * 100, 2)))

## feature scaling
def feature_scaling(df):
    '''Log transform all numeric cols.
    '''
    # scale remaining cols
    if isinstance(df, DataFrame):
        for col in count_columns:
            df = df.withColumn(col, F.log(df[col] + 1))
        for col in scale_columns:
            df = df.withColumn(col, F.log(df[col] + 1))
    else:
        df[count_columns] = df[count_columns].apply(lambda x: np.log(x + 1))
        df[scale_columns] = df[scale_columns].apply(lambda x: np.log(x + 1))

    return df


def add_high_low_flag(_data):
    high_low_classifier = KMeansModel.load('KMeans_model')
    # add high-low predictions
    _data = _data.withColumn('non_passive_events',
                             F.log(_data.frequency -
                                   (_data.DeleteEvent_count + 
                                    _data.GollumEvent_count +
                                    _data.IssueCommentEvent_count +
                                    _data.MemberEvent_count +
                                    _data.WatchEvent_count) + 1)
                            )

    _data = _data.withColumn('public_repos_gists',
                             F.log(_data.public_repos_count + _data.public_gists_count) + 1)

    stages = [VectorAssembler(inputCols=['non_passive_events', 'public_repos_gists'], 
                                outputCol="KMeans_features").setHandleInvalid("skip")]

    pipeline = Pipeline(stages = stages)
    pipelineModel = pipeline.fit(_data)
    _data = pipelineModel.transform(_data)
    _data = high_low_classifier.transform(_data)
    return _data


def get_contributors(gh, owner, repo):
    repo = gh.repository('numpy', 'numpy')
    return [[contributor.login, contributor.contributions] for contributor in repo.contributors()]


def create_query_from_list(user_list):
    '''
    '''
    txt = (
    """
    SELECT  
          actor.login,
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
          SUM(CASE WHEN type = 'WatchEvent' then 1 else 0 end) as WatchEvent_count

    FROM (
      SELECT public, type, repo.name, actor.login, created_at,
        JSON_EXTRACT(payload, '$.action') as event, 
      FROM (TABLE_DATE_RANGE([githubarchive:day.] , 
        TIMESTAMP('2018-08-01'), 
        TIMESTAMP('2018-12-31')
      ))
      )
  
    WHERE (actor.login = '""")

    end = ("""' AND public)
    GROUP by actor.login;

    """)

    nusers = len(user_list)
    for i, user in enumerate(user_list):
        if i < nusers - 1:
            txt += user + """' OR actor.login = '""" 
        else:
            txt += user + end
    return txt


def get_gh_credentials():
    f = open('gh-password.txt', 'r') 
    return f.read()[:-1]


def eval_metrics(prediction, label=None):
    if isinstance(prediction, DataFrame) and label is None:
        predictions = prediction.toPandas()
        label = predictions.label
        prediction = predictions.prediction
        
    TP = np.sum((label == 1) & (prediction == 1))
    FP = np.sum((label == 0) & (prediction == 1))
    FN = np.sum((label == 1) & (prediction == 0))
    TN = np.sum((label == 0) & (prediction == 0))
    precision = TP / (TP + FP)
    recall = TP / (TP + FN)
    accuracy = (TP + TN) / (TP + FP + FN + TN)
    f1score = 2 * (precision * recall) /  (precision + recall)
    print('Precision: {0}'.format(np.round(precision, 3)))
    print('Recall:    {0}'.format(np.round(recall, 3)))
    print('Accuracy:  {0}'.format(np.round(accuracy, 3)))
    print('F1-score:  {0}'.format(np.round(f1score, 4)))


def add_time_columns(df, end_date='2016-06-01 23:59:59+00:00'):
    
    df['created_at'] = pd.to_datetime(df.created_at[:10], errors='coerce')
    df['last_event'] = pd.to_datetime(df.last_event[:10], errors='coerce')
    df['first_event'] = pd.to_datetime(df.first_event[:10], errors='coerce')
    
    end_date = pd.to_datetime(end_date[:10])
    print(df.created_at)
    print(end_date)
    df['T'] = np.round((end_date - df.created_at) / pd.Timedelta(days = 1))
    df['recency'] = (df.last_event - df.created_at)  / pd.Timedelta(days = 1)
    df['time_between_first_last_event'] = (df.last_event - df.first_event) / pd.Timedelta(days = 1)
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
    
    second_period = second_period.withColumn('event_count', 
                             sum(second_period[col] for col in second_period.columns if col in count_columns))
    
    churn_data = churn_data.withColumn('frequency', 
                             sum(churn_data[col] for col in churn_data.columns if col in count_columns))
    
    second_period_event_count = second_period.selectExpr(
        "actor as login", "event_count as second_period_event_count")
    
    churn_data = churn_data.join(second_period_event_count,
                                 on='login', how='left')
    churn_data = churn_data.fillna(0, subset='second_period_event_count')

    f_datestring=udf.UserDefinedFunction(lambda x: x[:-4] + '+00:00', StringType())

    churn_data = churn_data.withColumn("first_event", f_datestring(churn_data.first_event))
    churn_data = churn_data.withColumn("last_event", f_datestring(churn_data.last_event))
    
    churn_data = churn_data.withColumn("first_event", to_timestamp(churn_data.first_event))
    churn_data = churn_data.withColumn("last_event", to_timestamp(churn_data.last_event))
    churn_data = churn_data.withColumn("created_at", to_timestamp(churn_data.created_at))
    churn_data = churn_data.withColumn("updated_at", to_timestamp(churn_data.updated_at))
    
    churn_data = churn_data.withColumn("recency", datediff(churn_data.last_event, 
                                                           churn_data.created_at))
    churn_data = churn_data.withColumn("time_between_first_last_event", 
                                   datediff(churn_data.last_event, churn_data.first_event))

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
                   'hireable', 'recency',
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


def get_repo_contrib_history(gh, gc_client, owner, repo):
    '''
    '''
    contributors = get_contributors(gh, owner, repo)
    contributors_df = pd.DataFrame(contributors, columns=['login', 'repo_contributions_count'])
    contributors_list = np.asarray(contributors)[:, 0]
    
    query = create_query_from_list(contributors_list)

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = True
    df = gc_client.query(query, location="US", job_config=job_config).to_dataframe()
    
    contributors_df = pd.DataFrame(contributors, columns=['login', 'repo_contributions_count'])
    contributors_df = contributors_df.merge(df, right_on='actor_login', left_on='login', how='left')
    contributors_df[count_columns] = contributors_df[count_columns].fillna(0).astype(int)
    contributors_df['event_count'] = contributors_df.event_count.fillna(0)
    
    return contributors_df


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