A = LOAD 'book-crossing/BX-Users-Prepro.txt' USING PigStorage(';')  AS (userid:int, location:chararray, age:int);
A = LOAD 'book-crossing/BX-Ratings-Prepro.txt' USING PigStorage(';')  AS (userid:int, location:chararray, age:int);
user_id INT, 
  isbn STRING, 
  rating INT
B = FILTER A BY gni > 2000;
C = ORDER B BY gni;

D = load 'export-data.csv' using PigStorage(',')  AS (country:chararray, expct:float); 

E = JOIN C BY country, D by country;

dump E; 
