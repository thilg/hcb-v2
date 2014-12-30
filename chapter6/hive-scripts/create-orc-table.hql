USE bookcrossing;

CREATE TABLE IF NOT EXISTS users_orc 
  (user_id INT, 
  location STRING, 
  age INT) 
COMMENT 'Book Crossing users table ORC format' 
STORED AS ORC;

INSERT INTO TABLE users_orc 
  SELECT * 
  FROM users;