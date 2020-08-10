'''
Author: Varun Chopra
Date: 04-06-2020

This Py file is responsible for extracting NSD log data from the directory tree and storing them in an organised manner in a CSV file.
Two methods are defined below, with their purposes described in comments.
The extracted logs are stored in a newly created file, logsAll.csv.
'''

import os
import csv
import re


# Method to traverse directory and get path to each log file
def traverse_directory(path):

	files = []
	# r=root, d=directories, f = files
	for r, d, f in os.walk(path):
		for file in f:
			if '.txt' in file:
				files.append(os.path.join(r, file))

	files = sorted(files)

	return files


# Method to extract logs from each log file
def extract_queries(logs):

	# Write header to CSV file
	header = 'SessionID Timestamp IntentName Event UserInput Response'
	header = header.split()
	file = open('../Intermediate Log Data/logsAll.csv', 'w', newline='')
	with file:
	    writer = csv.writer(file)
	    writer.writerow(header)

	# Extraction of log data
	for log in logs:

		# Skip macOS Desktop Services Store files
		if '.DS_Store' in log:
			continue

        # Split log file path with '/' delimiter
		logSplit = log.split('/')
        # Getting log file name
		session = logSplit[6]
        # Removing '-WebChatBot.txt' to get Session ID
		session = session[:-15]

        # Opening and reading log file
		f = open(log, "r")
		chat = f.read()
		chat = chat.replace('\n','')

        # Data Anonymisation
		chat = re.sub('[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+','abc@xyz.com',chat)
		chat = re.sub('[0-9]{10}','9XXXXXXXXX',chat)

		# Split chat with '||' delimiter
		chatSplit = chat.split('||')

		timeFirstQuery = chatSplit[1]
		timeFirstQuery = timeFirstQuery.split('#')
		firstQuery = timeFirstQuery[1]

        # When user enters query before welcome intent
		if firstQuery != 'User:' and firstQuery[:5] != 'Vani:':
			i=0
        # When user enters query after welcome intent
		elif firstQuery[:5] == 'Vani:':
			i=2
        # When interaction begins with an empty query from user, followed by VANI's greeting.
		elif firstQuery == 'User:':
			i=3
        #checking for some other form of data
		else:
			print('additional data')

		# Every iteration of the following loop extracts a single query/response pair from the current log file
		while i+2 < len(chatSplit):

			# queryType contains Intent and Event details
			queryType = chatSplit[i]

			# Avoiding irrelevant interactions
			if queryType in ['#Greetings','#Default Welcome Intent:Left_at_welcome','#WelcomeIntent','#Default Welcome Intent']:
				i+=3

			event = ''
			intent = ''

			# Getting Intent and Event from queryType
			queryType = queryType.split(':')
			intent = queryType[0]
			if len(queryType) > 1:
				event = queryType[1]

			# timeQuery contains timestamp and query
			timeQuery = chatSplit[i+1]

			timeQuery = timeQuery.split('#')
			timestamp = timeQuery[0]
			query = timeQuery[1]

			response = chatSplit[i+2]

			# Removing irrelevant parts of different fields
			intent = intent.replace('#','')
			query = query.replace('User:','')
			response = response.replace('Vani:','')

			# Writing to csv file
			to_append = [session,timestamp,intent,event,query,response]
			file = open('../Intermediate Log Data/logsAll.csv', 'a', newline='')
			with file:
				writer = csv.writer(file)
				writer.writerow(to_append)

			i+=3


if __name__ == '__main__':

	path = '../nsdChatBot'

	#Calling traverse_directory method
	logs = traverse_directory(path)

	print('Extracting log data...')
	#Calling extract_queries method
	extract_queries(logs)

	print('Logs extracted successfully to logsAll.csv')
