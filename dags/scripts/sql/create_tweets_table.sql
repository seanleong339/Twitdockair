-- Creates table to store tweets if it does not exist
CREATE TABLE IF NOT EXISTS tweets(
	id VARCHAR PRIMARY KEY,
	created_date DATE,
	created_time TIME,
	content VARCHAR,
	username VARCHAR,
	retweetCount INT,
	likeCount INT
);