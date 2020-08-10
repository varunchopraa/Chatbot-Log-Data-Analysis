'''
Author: Varun Chopra
Date: 04-06-2020

This Py file is responsible for finalising the segregation of the logs into two categories:
    1. Domain specific 
    2. General
Elasticsearch match query is used to segregate the logs based on the entity keywords.
One class and one method are defined below, with their purposes described in comments.
Domain specific logs are stored in DomainFinal.csv.
General logs are stored in GeneralFinal.csv.

NOTE: This module only exists to be called by segregate_success module.
'''

from elasticsearch import Elasticsearch, helpers
import pandas as pd
import segregate_domain as sd


# Class for Elasticsearch keyword match
class KeywordSearch(object):

	# Instantiating Elasticsearch and defining index
    def __init__(self, index):
        self.elasticsearch = Elasticsearch()
        self.index = index

    # Class method for Elasticsearch match query to further filter relevant log data 
    def search(self, keywords, df):
        query = {
            "query": {
                "bool": {
                    "should": []
                }
            },
            "size": 45000
        }
        
        # Append clause for each keyword to query.
        # Results must contain any one of the entity keywords in UserInput field
        for keyword in keywords:
            query["query"]["bool"]["should"].append({
                "match": {
                    "UserInput": {
                        "query": keyword,
                        "operator": "and",
                        "fuzziness": "AUTO"
                    }
                }
             })

        # Run elasticsearch query
        results_gen = helpers.scan(self.elasticsearch, query=query, index=self.index)
        results = list(results_gen)
        
        # Appending results to dataframe
        for res in results:
            df = df.append(res['_source'], ignore_index=True)
        
        return df


# Method to get general log data classified as domain specific data by taking difference of domain specifc data before and after calling search method of KeywordSearch class
def finalise_segregation():

	# Reading CSV files
	gen = pd.read_csv('../Intermediate Log Data/logsGeneralES.csv')
	dom = pd.read_csv('../Intermediate Log Data/logsDomainES.csv')
	domfinal = pd.read_csv('../Intermediate Log Data/logsDomainESFinal.csv')

	# Getting general queries wrongly classified as domain specific
	domtogen = dom.merge(domfinal, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
	domtogen = domtogen.drop('_merge', axis=1)

	# Append to general logs csv
	genfinal = pd.concat([gen, domtogen])
	# Moving emails from general to domain specific
	email = genfinal[genfinal.UserInput.str.contains('abc@xyz.com', na=False, case=False)]

	# Moving 'no' and 'hey' user inputs from domain specific to general log data
	no_hey = domfinal[(domfinal.UserInput.str.lower() == 'no') | (domfinal.UserInput.str.lower() == 'hey') | (domfinal.UserInput.str.lower() == 'nope')]
	genfinal = pd.concat([genfinal, no_hey])
	domfinal = domfinal[(domfinal.UserInput.str.lower() != 'no') & (domfinal.UserInput.str.lower() != 'hey') & (domfinal.UserInput.str.lower() != 'nope')]

	# Adding email data to domain specific
	domfinal = pd.concat([domfinal, email])
	domfinal.to_csv('../Intermediate Log Data/DomainFinal.csv', index=False)

	# Removing email data from general
	genfinal = genfinal.merge(email, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
	genfinal = genfinal.drop('_merge', axis=1)

    # Removing entries with only 'yes' and 'no' in UserInput
	genfinal = genfinal[(genfinal['UserInput'].str.lower() != 'yes') & (genfinal['UserInput'].str.lower() != 'no')]

	genfinal.to_csv('../Intermediate Log Data/GeneralFinal.csv', index=False)


# Main method to be called by segregate_success module
def main():

    # Getting entity keywords using get_keywords method of segregate_domain module
    searchKey = sd.get_keywords()

    print('Finalising domain segregation...')
    # Creating empty dataframe
    df = pd.DataFrame(columns=['SessionID', 'Timestamp', 'IntentName','Event','UserInput','Response'])
    # Calling search method of KeywordSearch class
    df = KeywordSearch(index="logsdomain").search(keywords=searchKey, df=df)
    # Removing duplicates
    df.drop_duplicates(keep=False,inplace=True)
    # Writing to csv file
    df.to_csv('../Intermediate Log Data/logsDomainESFinal.csv', index=False)
    # Calling finalise_segregation method
    finalise_segregation()
    print('Segregation successful. Data stored in GeneralFinal.csv and DomainFinal.csv')

