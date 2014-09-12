B = LOAD 'book-crossing/BX-Books-prepro.txt' USING PigStorage(';')  AS (
	isbn:chararray, 
	title:chararray, 
	author:chararray, 
	year:int, 
	publisher:chararray, 
	image_s:chararray, 
	image_m:chararray, 
	image_l:chararray);
R = LOAD 'book-crossing/BX-Book-Ratings-Prepro.txt' USING PigStorage(';')  AS (
	userid:int, 
	isbn:chararray, 
	ratings:int);
RA = FILTER R BY ratings > 3;
J = JOIN B BY isbn, RA by isbn;
JA = group J by author;
JB = foreach JA generate COUNT(J), group;
OA = LIMIT JB 100;
dump OA; 
