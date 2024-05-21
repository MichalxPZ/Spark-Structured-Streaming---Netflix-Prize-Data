DROP DATABASE IF EXISTS :"db_name";
DROP USER IF EXISTS :"user";
CREATE DATABASE :"db_name" WITH ENCODING 'UTF8';
CREATE USER :"user" WITH PASSWORD :'password';
GRANT ALL PRIVILEGES ON DATABASE :"db_name" TO :"user";

\c :"db_name";
DROP TABLE IF EXISTS movie_ratings;
CREATE TABLE IF NOT EXISTS movie_ratings (
     window_start BIGINT NOT NULL,
     movie_id VARCHAR(32) NOT NULL,
     title VARCHAR(128) NOT NULL,
     rating_count INTEGER NOT NULL,
     rating_sum INTEGER NOT NULL,
     unique_rating_count INTEGER NOT NULL,
     PRIMARY KEY (window_start, movie_id)
);
