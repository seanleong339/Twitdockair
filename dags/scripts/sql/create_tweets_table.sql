-- Creates table to store tweets if it does not exist
CREATE TABLE IF NOT EXISTS tweets(
	id VARCHAR PRIMARY KEY,
	date_time VARCHAR,
	content VARCHAR,
	username VARCHAR,
	retweetCount INT,
	likeCount INT
);