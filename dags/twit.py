from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator
import snscrape.modules.twitter as sntwitter
import pandas as pd
import os
from datetime import datetime, timedelta
import time
from dotenv import dotenv_values
from sqlalchemy import create_engine, inspect

#CONFIG = dotenv_values('.env')
#if not CONFIG:
#    CONFIG = os.environ
#
#connection_uri = "postgresql+psycopg2://{}:{}@{}:{}".format(
#    CONFIG["POSTGRES_USER"],
#    CONFIG["POSTGRES_PASSWORD"],
#    CONFIG['POSTGRES_HOST'],
#    CONFIG["POSTGRES_PORT"],
#)
#
#engine = create_engine(connection_uri, pool_pre_ping=True)
#engine.connect()

#AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

def extract_tweet_data():
	location = "1.352083, 103.819839, 20km"
	curr = int(time.time())
	dt = datetime.fromtimestamp(curr)
	tweets = []
	for i, tweet in enumerate(
		sntwitter.TwitterSearchScraper(
			'geocode:"{}" since_time:{} until_time:{}'.format(location, curr-900, curr)
			).get_items()
		):
		if i > 100:
			break
		tweets.append([tweet.id, tweet.date, tweet.content, tweet.user.username])
	df = pd.DataFrame(tweets, columns=["id", "datetime", "content", "user"])
	df.to_csv(f'data/{dt}.csv')

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
	python_callable=extract_tweet_data
)
