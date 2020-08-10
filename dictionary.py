'''
Author: Varun Chopra
Date: 04-06-2020

This Py file is responsible for performing dictionary check on log data and retaining only those queries which contain at least one meaningful word.
Dictionary check is performed through Wiktionary API. 
Since performing an API GET request for every word in each query would take a lot of time, multiprocessing has been employed to speed up this task.
Four methods are defined below, with their purposes described in comments.
Logs passing the dictionary check are stored in a newly created file, logsAllDict.csv.
Discarded logs are stored in DictTrash.csv.
'''

import pandas as pd
import requests
import numpy as np
import multiprocessing
from multiprocessing import Pool, Process
import re


# Method to read pandas dataframe
def read_data(file):
	data = pd.read_csv(file)
	return data

# Multiprocessing
num_partitions = 8 		# Number of partitions to split dataframe
num_cores = 8 			# Number of cores on your machine


# Method to split dataframe into chunks and assign the task of dictionary checking them to different processes
def parallelize_dataframe(df, func):

	# Split dataframe into chunks
	df_split = np.array_split(df, num_partitions)
	# Use Pool from Py microprocessing library
	pool = Pool(num_cores)
	df = pd.concat(pool.map(func, df_split))
	pool.close()
	pool.join()

	return df


# Method to perform dictionary check on a chunk of data using Wiktionary API
def dictionary_check(chunk):

	# Using global dataframe to ensure no overwiting occurs
	global df 				

	for index, row in chunk.iterrows():
		# Get query from UserInput field
		query = str(row['UserInput'])
		# Removing special characters
		query = re.sub(r"[^a-zA-Z0-9]+", ' ', query)
		# Split query with whitespace delimiter
		words = str(query).split()

		for word in words:
			# Wiktionary API GET request
			response = requests.get('https://en.wiktionary.org/w/api.php?action=query&titles=' + word.lower() + '&format=json')
			resData = response.json()

			# Write queries with even one meaningful word to global dataframe
			if list(resData['query']['pages'].keys())[0] != '-1':
				df = df.append(row)
				break

	return df


# Method to store discarded logs
def discarded(data):

	dataCleaned = pd.read_csv('../Intermediate Log Data/logsAllDict.csv')
	dataRemoved = data.merge(dataCleaned, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
	dataRemoved = dataRemoved.drop('_merge',axis=1)
	dataRemoved.to_csv('../Intermediate Log Data/DictTrash.csv', index=False)


if __name__ == '__main__':

	file = '../Intermediate Log Data/logsAll.csv'
	# Get log data in a dataframe
	data = read_data(file)

	# Creating new global dataframe
	df = pd.DataFrame(columns=['SessionID', 'Timestamp', 'IntentName','Event','UserInput','Response'])

	print('Performing dictionary check on log data...')

	# Calling dictionary_check method through different processes on different chunks of data
	dataDict = parallelize_dataframe(data, dictionary_check)
	# Writing datafram to csv file
	dataDict.to_csv('../Intermediate Log Data/logsAllDict.csv', index=False)
	dataDict.to_csv('../To Index/logsAllDict.csv', index=False)
	print('Dictionary check complete. Valid data stored in logsAllDict.csv')

	# Calling discarded method
	discarded(data)
	print('Discarded logs stored in DictTrash.csv')

