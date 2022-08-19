from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
import snscrape.modules.twitter as sntwitter
import pandas as pd
import os
from datetime import datetime, timedelta
import time
from dotenv import dotenv_values
from sqlalchemy import create_engine, inspect

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
		tweets.append([tweet.id,
		 		tweet.date,
				tweet.content,
				tweet.user.username, 
				tweet.retweetCount, 
				tweet.likeCount])
	df = pd.DataFrame(tweets, columns=["id", "datetime", "content", "username", "retweets", "likes"])
	if not os.path.exists(f"{dagpath}/processed_data"):
		os.mkdir(f"{dagpath}/processed_data")
	df.to_csv(f'{dagpath}/processed_data/{dt}.csv', 
		index=False, 
		header=True	
		)
	task_instance = kwargs['task_instance']
	task_instance.xcom_push(key='filename',value=f'{dagpath}/processed_data/{dt}.csv')
	

def remove_tempdata(**kwargs):
	task_instance = kwargs['ti']
	os.remove({{task_instance.xcom_pull(task_ids='extract_tweets', key='filename')}})


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

load_tweets = PostgresOperator(
	dag=dag,
	task_id="load_tweets",
	sql="./scripts/sql/load_tweets.sql",
#	parameters={
#		"filename": {{task_instance.xcom_pull(task_ids='extract_tweets',key='filename')}}
#	}
	postgres_conn_id="postgres_tweets",
)

remove_tempdata = PythonOperator(
	dag=dag,
	task_id="remove_temp_files",
	python_callable=remove_tempdata,
	#op_args=[filename],
	provide_context=True
)

create_table >> extract_tweets >> load_tweets >> remove_tempdata
