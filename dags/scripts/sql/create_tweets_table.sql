-- Creates table to store tweets if it does not exist
CREATE TABLE IF NOT EXISTS tweets(
	id VARCHAR PRIMARY KEY,
	date_time DATE,
	content VARCHAR,
	username VARCHAR
);