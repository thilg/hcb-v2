#!/bin/bash
set -e
wget http://www.textfixer.com/resources/common-english-words-	with-contractions.txt
mkdir –p /home/Hadoop/stopwords
mv common-english-words-with-contractions.txt 	/home/Hadoop/stopwords
