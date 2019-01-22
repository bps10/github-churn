import pandas as pd
import numpy as np
import matplotlib.pylab as plt
import pandas as pd
import os

from pyspark.sql import SparkSession, udf
from pyspark.sql.functions import to_timestamp, datediff
from pyspark.sql.types import IntegerType, FloatType, DoubleType, BooleanType


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
    fullfile = os.path.join("trees", filename, ".txt")
    text_file = open(fullfile, "w")
    text_file.write(rfModel.toDebugString)
    text_file.close()
    print('Saved to fullfile')

    
def get_merged_data(appName='gh-churn'):
    
    spark = SparkSession.builder.appName(appName).getOrCreate()
    first_period = spark.read.csv('events_data/events_2016_01_01_2016_06_01.csv', 
                                  header = True, inferSchema = True)    
    second_period = spark.read.csv('events_data/events_2016_06_02_2016_11_01_01.csv', 
                                   header = True, inferSchema = True)
    users = spark.read.csv('user_data/all_users.csv', header=True, inferSchema=True)
    users = users.drop('event_count').drop('last_event').drop('first_event').drop('_c0')
    
    churn_data = users.join(first_period, users['login'] == first_period['actor'], 
                            how='left')
    second_period_event_count = second_period.selectExpr(
        "actor as login", "event_count as second_period_event_count")
    
    churn_data = churn_data.join(second_period_event_count,
                                 on='login', how='left')
    '''
    churn_data = churn_data.withColumn("first_event", to_timestamp(churn_data.first_event))
    churn_data = churn_data.withColumn("last_event", to_timestamp(churn_data.last_event))
    churn_data = churn_data.withColumn("created_at", to_timestamp(churn_data.created_at))
    churn_data = churn_data.withColumn("updated_at", to_timestamp(churn_data.updated_at))
    
    churn_data = churn_data.withColumn("time_between_first_last_event", 
                                   datediff(churn_data.last_event, churn_data.first_event))
    '''
    
    churn_data = churn_data.withColumn("public_repos_count", churn_data.public_repos_count.cast(IntegerType()))
    churn_data = churn_data.withColumn("public_gists_count", churn_data.public_gists_count.cast(IntegerType()))
    churn_data = churn_data.withColumn("followers_count", churn_data.followers_count.cast(IntegerType()))
    churn_data = churn_data.withColumn("following_count", churn_data.following_count.cast(IntegerType()))

    churn_data = churn_data.fillna(0, subset='second_period_event_count')
    
    f_udf=udf.UserDefinedFunction(lambda x: 1 if x is not None else 0, IntegerType())

    churn_data = churn_data.withColumn("blog", f_udf(churn_data.blog))
    churn_data = churn_data.withColumn("company", f_udf(churn_data.company))
    churn_data = churn_data.withColumn("hireable", f_udf(churn_data.hireable))
    
    churn_data = churn_data.withColumnRenamed("event_count", "frequency")

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
    
    # remove outliers with very high number of Events in early 2016.    
    n_total_users = churn_data.count()
    churn_data = churn_data.filter(churn_data.frequency < 200)
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




# curl -u "bps10" -i https://api.github.com/users?page=50&per_page=100
def get_user_info_(user):
    '''Depreciated.
    '''
    buffer = BytesIO()
    c = pycurl.Curl()
    c.setopt(c.URL, 'https://api.github.com/users/{0}'.format(user))
    c.setopt(c.WRITEDATA, buffer)
    c.perform()
    c.close()

    body = buffer.getvalue()
    # Body is a byte string.
    # We have to know the encoding in order to print it to a text file
    # such as standard output.
    body.decode('iso-8859-1')

    return dict(json.loads(d))


if __name__ == '__main__':
    df = get_merged_data()
    print(df.head())