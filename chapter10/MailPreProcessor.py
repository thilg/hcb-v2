#!/usr/bin/python

import sys;
import re;

def main(argv):

	list = []
	messageID =""
	subject =""
	fromAddress =""
	newsgroup =""
	doneHeaders = False

	line =  sys.stdin.readline();
	while line:
	    line = line.strip()
	    line = re.sub('\t',' ',line)
	    if (doneHeaders):
		list.append( line )
	    elif line.find( "From:" )!= -1:
		fromAddress = line [ len("From:"):]
	    elif line.find( "Message-ID:" ) != -1:
		messageID = line[ len("Message-ID:"):]
	    elif line.find( "Subject:" ) != -1:
		subject = line
	    elif line.find( "Newsgroups:" ) != -1:
		newsgroup = line[ len("Newsgroups:"):]
	    elif line == "":
		doneHeaders = True
	 
	    line = sys.stdin.readline();
	 
	value = ' '.join( list )
	value = fromAddress + "\t" + newsgroup +"\t" + subject +"\t" + value
	print '%s\t%s' % (messageID, value)


if  __name__ == "__main__":
	main(sys.argv)
