'''
Author: Varun Chopra
Date: 04-06-2020

This Py file is responsible for segregating unsuccessfully answered domain specific log data into queries which could not be answered at first but were successfully answered later on.
Elasticsearch match_phrase query is used on unsuccessful and successful log data.
Timestamps are then compared and the unsuccessful-successful log pair with successful timestamp greater than unsuccessful are appended to dataframe.
One method is defined below, with its purpose described in comments.
Data is stored in Task3.csv.
'''

from elasticsearch import Elasticsearch, helpers
import pandas as pd
from datetime import datetime
import segregate_domain as sd


# Creating an Elasticsearch instance
es = Elasticsearch()


# Method to get log data which could not be answered at first but was successfully answered later on
def improvement_check(domUns, domIndex):

	df = pd.DataFrame(columns=['SessionID', 'Timestamp', 'IntentName','Event','UserInput','Response','SessionID2', 'Timestamp2', 'IntentName2','Event2','UserInput2','Response2'])

	# Iterating through every row in domUns and running match_phrase query on 'domsuccess' index
	for index, row in domUns.iterrows(): 
		res = es.search(index=domIndex,
	    					body= {
	    						"query": {
									"match_phrase": {
										"UserInput": {
											"query": row['UserInput']
										}
									}
								}
							}
						)
	    
		for hit in res['hits']['hits']:

			# Converting timestamps to datetime objects for comparison
			timestampUns = datetime.strptime(row['Timestamp'], '%m/%d/%Y %I:%M:%S %p')
			timestampSuc = datetime.strptime(hit['_source']['Timestamp'], '%m/%d/%Y %I:%M:%S %p')
	        
	        # Comparing timestamps
			if timestampSuc > timestampUns:
	            # Append both unsuccessful and subsequently successful log data to dataframe
				to_append = list(row) + list(hit['_source'].values())

				df_length = len(df)
				df.loc[df_length] = to_append
	# Dropping duplicate entries
	df.drop_duplicates(keep='first',inplace=True)

	return df



if __name__ == '__main__':

	# Reading unsuccessful domain specific data
	domUns = pd.read_csv('../Intermediate Log Data/DomainUnsuccessful.csv')

	print('Segregating domain specific queries that could not be answered at first but were answered successfully later on...')
	# Calling improvement_check method
	task3 = improvement_check(domUns, 'domsuccess')

	# Writing to CSV file
	task3.to_csv('../Intermediate Log Data/Task3.csv', index=False)
	task3.to_csv('../To Index/Task3.csv', index=False)

	# Getting queries which could still not be answered on a later try
	task3neg = improvement_check(domUns, 'domunsuccess')
	task3neg.to_csv('../Intermediate Log Data/Task3Neg.csv', index=False)

	print('Segregation successful. Data stored in Task3.csv.')


