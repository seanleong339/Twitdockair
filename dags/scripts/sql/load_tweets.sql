COPY tempdata 
FROM  {{task_instance.xcom_pull(task_ids='extract_tweets',key='filename')}}
WITH (FORMAT CSV, HEADER);

INSERT INTO tweets(id, date_time, content, username, retweetCount, likeCount)
VALUES (
	SELECT *
	FROM tempdata
	WHERE id NOT IN (
		SELECT id FROM tweets
	) 
);

DROP TABLE tempdata;