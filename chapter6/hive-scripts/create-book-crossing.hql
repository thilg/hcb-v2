CREATE DATABASE IF NOT EXISTS bookcrossing;

USE bookcrossing;

CREATE TABLE IF NOT EXISTS books
  (isbn STRING, 
  title STRING, 
  author STRING, 
  year INT, 
  publisher STRING, 
  image_s STRING, 
  image_m STRING, 
  image_l STRING) 
COMMENT 'Book crossing books list cleaned'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\073' 
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '${hiveconf:DATA_DIR}/BX-Books-Prepro.txt' OVERWRITE INTO TABLE books;

CREATE TABLE IF NOT EXISTS users 
  (user_id INT, 
  location STRING, 
  age INT) 
COMMENT 'Book Crossing users cleaned' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\073' 
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '${hiveconf:DATA_DIR}/BX-Users-Prepro.txt' OVERWRITE INTO TABLE users; 

CREATE TABLE IF NOT EXISTS ratings
  (user_id INT, 
  isbn STRING, 
  rating INT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\073' 
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '${hiveconf:DATA_DIR}/BX-Book-Ratings-Prepro.txt' OVERWRITE INTO TABLE ratings;
