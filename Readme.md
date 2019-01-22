
## Fetch Event data

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

## Merging datasets before reading into Pandas.

Install csvkit for manipulating multiple csv files.
```
sudo pip install csvkit
```

## Fetch user data

`Data_fetch.ipynb`

Merge csv files using [csvkit](https://csvkit.readthedocs.io/en/1.0.2/).
```
csvstack user_batch_* > all_users.csv
```

```
for old in *; do mv $old `basename $old `.csv; done;
```


## Visualize Random Trees

```
pip install eurekatrees
```
eurekatrees --trees ./trees/rf_tree.txt 
