from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
import snscrape.modules.twitter as sntwitter
import pandas as pd
import os
from datetime import datetime, timedelta
import time
from dotenv import dotenv_values
import sqlalchemy

dagpath = os.getcwd()

def extract_tweet_data(**kwargs):
	location = "1.352083, 103.819839, 20km"
	curr = int(time.time())
	dt = datetime.fromtimestamp(curr).strftime("%Y-%m-%d_%H%M")
	tweets = []
	for i, tweet in enumerate(
		sntwitter.TwitterSearchScraper(
			'geocode:"{}" since_time:{} until_time:{}'.format(location, curr-900, curr)
			).get_items()
		):
		if i > 100:
			break
		tweets.append([str(tweet.id),
		 		tweet.date,
				tweet.content,
				tweet.user.username, 
				tweet.retweetCount, 
				tweet.likeCount])
	df = pd.DataFrame(tweets, columns=["id", "datetime", "content", "username", "retweets", "likes"])
	df['created_date'] = pd.to_datetime(df['datetime']).dt.date
	df['created_time'] = pd.to_datetime(df['datetime']).dt.time
	del df['datetime']
	if not os.path.exists(f"{dagpath}/processed_data"):
		os.mkdir(f"{dagpath}/processed_data")
	df.to_csv(f'{dagpath}/processed_data/{dt}.csv', 
		index=False, 
		header=True	
		)
	task_instance = kwargs['task_instance']
	task_instance.xcom_push(key='filename',value=f'{dagpath}/processed_data/{dt}.csv')
	
def load_data(**kwargs):
	pg_hook = PostgresHook(postgres_conn_id="postgres_tweets")
	task_instance = kwargs['task_instance']
	filename = task_instance.xcom_pull(task_ids='extract_tweets', key='filename')
	df = pd.read_csv(filename)
	engine = sqlalchemy.create_engine('postgresql://postgres:postgres@database/tweets')
	df['id'] = df['id'].astype(str)
	df.to_sql("tempdata", engine, index=False, if_exists="replace", 
			dtype= {'id': sqlalchemy.types.String(),
				'created_date': sqlalchemy.types.Date(),
				'created_time': sqlalchemy.types.Time(),
				'content': sqlalchemy.types.String(),
				'username': sqlalchemy.types.String(),
				'retweetCount': sqlalchemy.types.Integer(),
				'likeCount': sqlalchemy.types.Integer()})
	insert_sql = open(f"{os.getcwd()}/dags/scripts/sql/load_tweets.sql")
	insert_sql= insert_sql.read()
	pg_hook.run(insert_sql)
	

def remove_tempdata(**kwargs):
	task_instance = kwargs['ti']
	os.remove(task_instance.xcom_pull(task_ids='extract_tweets', key='filename'))


default_args = {
	"owner" : "airflow",
	"start_date" : datetime(2022,8,17),
	"retries": 2,
    	"retry_delay": timedelta(minutes= 1),
}

dag = DAG(
	"get_tweets",
	default_args= default_args,
	schedule_interval= "0 * * * *",
	max_active_runs=1,
	catchup=False
)

create_table = PostgresOperator(
	dag=dag,
	task_id="create_table",
	sql="./scripts/sql/create_tweets_table.sql",
	postgres_conn_id="postgres_tweets",
)


extract_tweets = PythonOperator(
	dag=dag,
	task_id="extract_tweets",
	python_callable=extract_tweet_data,
	provide_context=True
)

load_tweets = PythonOperator(
	dag=dag,
	task_id="load_tweets",
	python_callable=load_data,
	provide_context=True
)

cleanup_temp = PythonOperator(
	dag=dag,
	task_id="cleanup_temp",
	python_callable=remove_tempdata,
	#op_args=[filename],
	provide_context=True
)

create_table >> extract_tweets >> load_tweets >> cleanup_temp
