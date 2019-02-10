# CodeChurn: Keeping open source projects active

## Motivation 
Open source development has produced millions of valuable products and tools that are used by engineers around the world. The success of these projects, however, depends on a large and active community of developers willing to donate their time. 

To get a sense of the overall health of this community, take for instance, the period between Jan-May 2016. During this time, there were 3.7 million users who had at least one open source event on github. The probelm is that over the next 5 months 48% of those users made zero contributions.

What is needed is a way to monitor and maintain the health of the community. Code Churn is a tool for identifying users who are likely to churn so that stakeholders, such as project leaders, can prevent it before it happens.


## Data sources

1. [GitHub User api](https://developer.github.com/v3/) via [github3.py](https://github.com/sigmavirus24/github3.py)
2. [GitHub Event archive](http://www.gharchive.org/) on [BigQuery](https://bigquery.cloud.google.com/welcome)

## Tech stack

1. Google cloud api
2. Pyspark
3. Pandas
4. Flask
5. EC2

## Notes

### SQL queries
#### Count events as a function of month

```sql
SELECT  
  COUNT(type) as event_count,
  EXTRACT(MONTH FROM created_at) AS created_at

FROM `githubarchive.year.2018`
   
WHERE (public)

GROUP BY created_at
ORDER BY created_at;
```

#### Count users active each month

```sql
SELECT  
      COUNT(DISTINCT(actor.login)) as number_active_users,
      EXTRACT(MONTH FROM created_at) AS created_at

    FROM `githubarchive.year.2015`
    
    WHERE public

GROUP BY created_at
ORDER BY created_at;
```

#### Fetch Event data

Github event data is archived nightly [here](http://www.gharchive.org/). [Data](https://bigquery.cloud.google.com/table/githubarchive:day.) are stored in a Google Big Table. The following command run in BigQuery will return count data for all events during the specified period.

```sql
/* also get a count of the differt types of commits */
SELECT actor.login as actor, 
  COUNT(actor) as event_count, 
  MAX(created_at) as last_event, 
  MIN(created_at) as first_event,
  SUM(CASE WHEN type = 'CheckRunEvent' then 1 else 0 end) as CheckRunEvent_count,
  SUM(CASE WHEN type = 'CheckSuiteEvent' then 1 else 0 end) as CheckSuiteEvent_count,
  SUM(CASE WHEN type = 'CommitCommentEvent' then 1 else 0 end) as CommitCommentEvent_count,
  SUM(CASE WHEN type = 'ContentReferenceEvent' then 1 else 0 end) as ContentReferenceEvent_count,
  SUM(CASE WHEN type = 'CreateEvent' then 1 else 0 end) as CreateEvent_count,
  SUM(CASE WHEN type = 'DeleteEvent' then 1 else 0 end) as DeleteEvent_count,
  SUM(CASE WHEN type = 'DeploymentEvent' then 1 else 0 end) as DeploymentEvent_count,
  SUM(CASE WHEN type = 'DeploymentStatusEvent' then 1 else 0 end) as DeploymentStatusEvent_count,
  SUM(CASE WHEN type = 'DownloadEvent' then 1 else 0 end) as DownloadEvent_count,
  SUM(CASE WHEN type = 'FollowEvent' then 1 else 0 end) as FollowEvent_count,
  SUM(CASE WHEN type = 'ForkEvent' then 1 else 0 end) as ForkEvent_count,
  SUM(CASE WHEN type = 'ForkApplyEvent' then 1 else 0 end) as ForkApplyEvent_count,
  SUM(CASE WHEN type = 'GitHubAppAuthorizationEvent' then 1 else 0 end) asGitHubAppAuthorizationEvent_count,
  SUM(CASE WHEN type = 'GistEvent' then 1 else 0 end) as GistEvent_count,
  SUM(CASE WHEN type = 'GollumEvent' then 1 else 0 end) as GollumEvent_count,
  SUM(CASE WHEN type = 'InstallationEvent' then 1 else 0 end) as InstallationEvent_count,
  SUM(CASE WHEN type = 'InstallationRepositoriesEvent' then 1 else 0 end) as InstallationRepositoriesEvent_count,
  SUM(CASE WHEN type = 'IssueCommentEvent' then 1 else 0 end) as IssueCommentEvent_count,
  SUM(CASE WHEN type = 'IssuesEvent' then 1 else 0 end) as IssuesEvent_count,
  SUM(CASE WHEN type = 'LabelEvent' then 1 else 0 end) as LabelEvent_count,
  SUM(CASE WHEN type = 'MarketplacePurchaseEvent' then 1 else 0 end) as MarketplacePurchaseEvent_count,
  SUM(CASE WHEN type = 'MemberEvent' then 1 else 0 end) as MemberEvent_count,
  SUM(CASE WHEN type = 'MembershipEvent' then 1 else 0 end) as MembershipEvent_count,
  SUM(CASE WHEN type = 'MilestoneEvent' then 1 else 0 end) as MilestoneEvent_count,
  SUM(CASE WHEN type = 'OrganizationEvent' then 1 else 0 end) as OrganizationEvent_count,
  SUM(CASE WHEN type = 'OrgBlockEvent' then 1 else 0 end) as OrgBlockEvent_count,
  SUM(CASE WHEN type = 'PageBuildEvent' then 1 else 0 end) as PageBuildEvent_count,
  SUM(CASE WHEN type = 'ProjectCardEvent' then 1 else 0 end) as ProjectCardEvent_count,
  SUM(CASE WHEN type = 'ProjectColumnEvent' then 1 else 0 end) as ProjectColumnEvent_count,
  SUM(CASE WHEN type = 'ProjectEvent' then 1 else 0 end) as ProjectEvent_count,
  SUM(CASE WHEN type = 'PublicEvent' then 1 else 0 end) as PublicEvent_count,
  SUM(CASE WHEN type = 'PullRequestEvent' then 1 else 0 end) as PullRequestEvent_count,
  SUM(CASE WHEN type = 'PullRequestReviewEvent' then 1 else 0 end) as PullRequestReviewEvent_count,
  SUM(CASE WHEN type = 'PullRequestReviewCommentEvent' then 1 else 0 end) as PullRequestReviewCommentEvent_count,
  SUM(CASE WHEN type = 'PushEvent' then 1 else 0 end) as PushEvent_count,
  SUM(CASE WHEN type = 'ReleaseEvent' then 1 else 0 end) as ReleaseEvent_count,
  SUM(CASE WHEN type = 'RepositoryEvent' then 1 else 0 end) as RepositoryEvent_count,
  SUM(CASE WHEN type = 'RepositoryImportEvent' then 1 else 0 end) as RepositoryImportEvent_count,
  SUM(CASE WHEN type = 'RepositoryVulnerabilityAlertEvent' then 1 else 0 end) as RepositoryVulnerabilityAlertEvent_count,
  SUM(CASE WHEN type = 'SecurityAdvisoryEvent' then 1 else 0 end) as SecurityAdvisoryEvent_count,
  SUM(CASE WHEN type = 'StatusEvent' then 1 else 0 end) as StatusEvent_count,
  SUM(CASE WHEN type = 'TeamEvent' then 1 else 0 end) as TeamEvent_count,
  SUM(CASE WHEN type = 'TeamAddEvent' then 1 else 0 end) as TeamAddEvent_count,
  SUM(CASE WHEN type = 'WatchEvent' then 1 else 0 end) as WatchEvent_count,

  FROM (
  SELECT public, type, repo.name, actor.login, created_at,
    JSON_EXTRACT(payload, '$.action') as event, 
  FROM (TABLE_DATE_RANGE([githubarchive:day.] , 
    TIMESTAMP('2018-01-01'), 
    TIMESTAMP('2018-06-01')
  )) 
  WHERE public #(type = 'IssuesEvent' AND public)
)
GROUP by actor;

```

After the query finishes, the resulting table must be exported to csv in google cloud storage. To do that:

1. In the BigQuery terminal, select "Job information" tab. Scroll down and select "Temporary Table". On the right side, select the "Export" tab and select "export to GCS". Choose save as location, file type and compression. In the case of tables larger than about 500 MB, the file name should end in `*.csv`. The `**` wildcard will instruct google cloud to break the table up over multiple files. 

2. Download each csv file from GCS.

3. Collect files into data directory.

4. `csvstack events_2018_01_01_2018_06_01* > events_02_02_2018_06_01.csv`

### Merging datasets before reading into Pandas.

Install csvkit for manipulating multiple csv files.
```
sudo pip install csvkit
```

### Fetch user data

`Data_fetch.ipynb`

Merge csv files using [csvkit](https://csvkit.readthedocs.io/en/1.0.2/).
```
csvstack user_batch_* > all_users.csv
```

```
for old in *; do mv $old `basename $old `.csv; done;
```


### Visualize Random Trees
[Eurekatrees](https://github.com/ChuckWoodraska/EurekaTrees)
```
pip install eurekatrees
```
eurekatrees --trees ./trees/rf_tree.txt 


### Installing Google Cloud SDK and PySpark on EC2

From [StackOverflow](https://stackoverflow.com/questions/44368263/install-google-cloud-sdk-on-aws-ec2):

1. `curl https://sdk.cloud.google.com | bash` 
2. restart shell: `exec -l $SHELL`
3. Copy pem file: `scp -i <keypair> myfile.txt ubuntu@ec2-x-x-x.com:` The colon at the end will ensure that it is copied to the home directory. Specify other locations directly after ec2-x-x-x.com/path/to/dir/. 
4. link account: `gcloud auth activate-service-account --key-file=$PATH TO KEY FILE` 
5. `gcloud init` (please make proper selection as per your requirement).
6. Install python 2.7 if not already present: `sudo apt install python2.7`
7. After above steps try running gsutil ls OR bq ls command and see if buckets and datasets are listed properly.
8. `pip install google-cloud-storage`

### Pyspark on EC2

[Instructions](https://medium.com/@josemarcialportilla/getting-spark-python-and-jupyter-notebook-running-on-amazon-ec2-dec599e1c297)

### Conda virtual env:

 Install: 
 1. `wget https://repo.continuum.io/archive/Anaconda3-5.0.1-Linux-x86_64.sh`
 2.  `bash Anaconda3-5.0.1-Linux-x86_64.sh`
 

1. `conda list --explicit > spec-file.txt`
2. spc 
3. `conda create --name myenv --file spec-file.txt`


### Bugs

[Port already in user](https://stackoverflow.com/questions/34457981/trying-to-run-flask-app-gives-address-already-in-use)
