Books = LOAD 'book-crossing/BX-Books-Prepro.txt' 
	USING PigStorage(';')  AS (
		isbn:chararray, 
		title:chararray, 
		author:chararray, 
		year:int, 
		publisher:chararray, 
		image_s:chararray, 
		image_m:chararray, 
		image_l:chararray);
Ratings = LOAD 'book-crossing/BX-Book-Ratings-Prepro.txt' 
	USING PigStorage(';')  AS (
		userid:int, 
		isbn:chararray, 
		ratings:int);
GoodRatings = FILTER Ratings BY ratings > 3;
J = JOIN Books BY isbn, GoodRatings by isbn;
JA = GROUP J BY author;
JB = FOREACH JA GENERATE COUNT(J), group;
OA = LIMIT JB 100;
DUMP OA; 
