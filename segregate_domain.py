'''
Author: Varun Chopra
Date: 04-06-2020

This Py file is responsible for segregating the logs into two categories:
	1. Domain specific 
	2. General
Elasticsearch match query is used to segregate the logs based on the entity keywords.
Five methods are defined below, with their purposes described in comments.
Domain specific logs are stored in logsDomainES.csv.
General logs are stored in logsGeneralES.csv.
'''

from elasticsearch import Elasticsearch, helpers
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import pandas as pd


# Creating an Elasticsearch instance
es = Elasticsearch()


# Method to get entity keywords from Elasticsearch index 'keywords'
def get_keywords():

	print('Getting keywords from Elasticsearch \'keywords\' index...')
	searchKey = list()

	# Using match_all query to get all entity keywords
	res = es.search(index="keywords", 
					body={
						"query": {
							"match_all": {}
						},
						"size": 3000
	 				})
	print("Received %d entity keywords.\n" % res['hits']['total']['value'])

	# Receieved keywords are appended to searchKey list
	for hit in res['hits']['hits']:
		searchKey.append("%(Attendance Marking Issue)s" % hit["_source"])

	return searchKey


# Method to remove log data with 'Ticket_Generated' Intent
def remove_ticket():

	print('Removing Ticket_Generated intent...')
	data = pd.read_csv('../Intermediate Log Data/logsAllDict.csv')

	# Dropping rows with NULL value in any field
	data = data.dropna()

	# Extracting a subset of dataframe by removing rows containing 'Ticket_Generated' Intent
	data = data[data['IntentName'] != 'Ticket_Generated']
	# Removing duplicate entries from the dataframe
	data.drop_duplicates(keep='first',inplace=True)
	print('Removed.\n')

	return data


# Method to preprocess keyword data
def keyword_prep(searchKey):

	print('Preprocessing keywords...')

	keywords = ''

	# Creating a single string containing all entity keywords
	for word in searchKey:
		keywords += word + ' '

	# Removing stopwords
	stop_words = set(stopwords.words('english'))
	word_tokens = word_tokenize(keywords)

	filtered_keywords = list()

	for w in word_tokens: 
		if w not in stop_words: 
			filtered_keywords.append(w)

	# Removing duplicate words from filtered_keywords
	filtered_keywords = list(dict.fromkeys(filtered_keywords))

	keywords = ''
	        
	# Recreating the single string containing relevant keyword data
	for word in filtered_keywords:
		keywords += word + ' '
	    
	print('Keywords preprocessed successfully.\n')

	return keywords


# Method to segregate domain specific queries 
def segregate_domain(keywords):

	print('Segregating domain specific queries...')
	# Creating new dataframe
	df = pd.DataFrame(columns=['SessionID', 'Timestamp', 'IntentName','Event','UserInput','Response'])

	# Elasticsearch match query on 'logsall' index
	# logsall is an index containing log data after dictionary check and remove_ticket()
	res = es.search(index="logsall",
						body=  {
							"query": {
								"match": {
									"UserInput": {
										"query": keywords,
										"operator": "or",
										"fuzziness": "AUTO"
										}
									}
								},
								"size": 45000
						}
					)

	# Results are appened to the dataframe
	for hit in res['hits']['hits']:
		df = df.append(hit['_source'], ignore_index=True)

	# Dropping duplicate entries
	df.drop_duplicates(keep='first',inplace=True)
	# Writing dataframe to csv file
	df.to_csv('../Intermediate Log Data/logsDomainES.csv', index=False)
	df.to_csv('../To Index/logsDomainES.csv', index=False)

	print('Domain specific queries successfully segregated and stored in logsDomainES.csv\n')


# Method to segregate general queries 
def segregate_general(keywords):

	print('Segregating general queries...')
	# Creating new dataframe
	df2 = pd.DataFrame(columns=['SessionID', 'Timestamp', 'IntentName','Event','UserInput','Response'])

	# Elasticsearch match query on 'logsall' index to get docs which do not match any of the keywords
	res = es.search(index="logsall",
						body=  {
							"query": {
	 							"bool": {
									"must_not": [
									{
										"match": {
											"UserInput": {
												"query": keywords,
												"operator": "or",
												"fuzziness": "AUTO"
											}
										}
									}
									]
								}
							},
							"size": 45000
						}
					)

	# Results are appened to the dataframe
	for hit in res['hits']['hits']:
		df2 = df2.append(hit['_source'], ignore_index=True)

	# Dropping duplicate entries
	df2.drop_duplicates(keep='first',inplace=True)

	# Writing dataframe to csv file
	df2.to_csv('../Intermediate Log Data/logsGeneralES.csv', index=False)

	print('General queries successfully segregated and stored in logsGeneralES.csv\n')



if __name__ == '__main__':

	# Calling get_keywords method
	searchKey = get_keywords()
	# Removing Ticket_Generated data
	data = remove_ticket()
	data.to_csv('../Intermediate Log Data/AllLogsFinal.csv', index=False)
	# Preprocessing keyword data
	keywords = keyword_prep(searchKey)
	# Segregating domain specific queries
	segregate_domain(keywords)
	# Segregating general queries
	segregate_general(keywords)
