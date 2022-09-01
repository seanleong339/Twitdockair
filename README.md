# Twitdockair

Twitdockair is a basic ETL project I started to familiarise with setting up Apache Airflow on Docker, as well as utilising Airflow as an orchestrator to build DAGs for a data pipeline. The current schedule of the pipeline is set to run every hour, and extracts 100 tweets at a time. 

![alt text](https://github.com/seanleong339/Twitdockair/blob/main/images/ui.PNG "ui")

The pipeline runs in 4 steps:

1. Creates the table 'tweets' in the database if it does not exist
2. Extract the tweets from Twitter api using the Snscrape library (https://github.com/JustAnotherArchivist/snscrape), and does simple transformation using Pandas to get date and time. The tweets are then saved as a csv file in the container.
3. The CSV file is loaded into the Postgresql database.
4. Cleaning up and removal of temporary data file created

## How to setup
Prerequisites: Docker

1. Clone the project into directory of your choice
2. If in Unix, set up the .env variables and docker network by running:
```
source .env
docker network create etl_network
docker-compose --env-file ./.env -f ./postgres-docker-compose.yaml up -d
```
If in Windows, run the script 'env.bat' in the folder 'setup', followed by running in command line:
```
docker network create etl_network
docker-compose --env-file ./.env -f ./postgres-docker-compose.yaml up -d
```

## How to run
1. In Unix, run in command line:
```
docker-compose -f airflow-docker-compose.yaml up -d
docker-compose -f postgres-docker-compose.yaml up -d
```
In Windows, run the batch file 'start.bat' in folder 'run'

2. Go to 'localhost:8080' in your browser, and use the username 'admin' password 'airflow' to access the Airflow GUI
3. When done with the pipeline, clean up and shut down by running 
```
docker-compose -f airflow-docker-compose.yaml down --volumes --rmi all
docker-compose -f postgres-docker-compose.yaml down --volumes --rmi all
```
or if in Windows run the 'cleanup.bat' file in folder 'run'
