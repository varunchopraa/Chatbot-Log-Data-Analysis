'''
Author: Varun Chopra
Date: 04-06-2020

This Py file is responsible for further segregating unsuccessfully answered domain specific log data and Task3 data.
One method is defined below, with its purpose described in comments.
Unsuccessfully answered domain specific log data and Task3 log data are further filtered using keyword match to get relevant data.
Filtered unsuccessfully answered domain specific log data is stored in FilteredDomainUnsuccess.csv
Filtered Task3 log data is stored in FilteredTask3.csv.
'''

from elasticsearch import Elasticsearch, helpers
import pandas as pd
from datetime import datetime
import finalise_segregation as fs
import segregate_domain as sd


def filter():
	# Getting entity keywords data
	searchKey = sd.get_keywords()

	print('Filtering unsuccessfully answered domain specific log data...')
	# Creating empty dataframe
	df = pd.DataFrame(columns=['SessionID', 'Timestamp', 'IntentName','Event','UserInput','Response'])
	# Filtering unsuccessful domain specific data using KeywordSearch class from final_segregation.py
	filteredUns = fs.KeywordSearch(index="domunsuccess").search(keywords=searchKey, df=df)
	# Removing duplicates
	filteredUns.drop_duplicates(keep=False,inplace=True)
	# Writing to CSV file
	filteredUns.to_csv('../Final Tasks Log Data/FilteredDomainUnsuccess.csv', index=False)

	print('Filtering Task3 log data...')
	# Creating empty dataframe
	df = pd.DataFrame(columns=['SessionID', 'Timestamp', 'IntentName','Event','UserInput','Response','SessionID2', 'Timestamp2', 'IntentName2','Event2','UserInput2','Response2'])
	# Filtering Task3 log data using KeywordSearch class from finalise_segregation.py
	filteredTask3 = fs.KeywordSearch(index="task3").search(keywords=searchKey, df=df)
	# Removing duplicates
	filteredTask3.drop_duplicates(keep=False,inplace=True)
	# Writing to CSV file
	filteredTask3.to_csv('../Final Tasks Log Data/FilteredTask3.csv', index=False)

	print('Filtered successfully.\nData stored in FilteredDomainUnsuccess.csv and FilteredTask3.csv.')



if __name__ == '__main__':

	filter()