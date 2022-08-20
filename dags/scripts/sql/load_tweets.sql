INSERT INTO tweets(id, created_date, created_time, content, username, retweetCount, likeCount)
SELECT id, created_date, created_time, content, username, retweets, likes
FROM tempdata
WHERE id NOT IN (
	SELECT id FROM tweets
);
DROP TABLE tempdata;