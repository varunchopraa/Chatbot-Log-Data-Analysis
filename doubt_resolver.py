'''
Author: Varun Chopra
Date: 04-06-2020

This Py file is responsible for segregating doubtful domain specific log data into unsuccessfully and successfully answered log data.
Elasticsearch match query is used on both UserInput and Response fields.
One class and three methods are defined below, with their purposes described in comments.
Successfully answered log data is stored in DomainSuccessful.csv.
Unsuccessfully answered log data is stored in DomainUnsuccessful.csv.
'''

from elasticsearch import Elasticsearch, helpers
import pandas as pd
import segregate_domain as sd


# Class for Elasticsearch match query on UserInput and Response fields
class KeywordSearch(object):

	# Instantiating Elasticsearch and defining index
    def __init__(self, index):
        self.elasticsearch = Elasticsearch()
        self.index = index

    # Class method for Elasticsearch match query
    def search(self, keywords, df):
        query = {
            "query": {
                "bool": {
                    "should": []
                }
            },
            "size": 45000
        }
        
        # Append must clause for each keyword to query.
        # Results must contain any one of the entity keywords in both UserInput and Response fields
        for keyword in keywords:
            query["query"]["bool"]["should"].append({
                            "bool": {
                                "must": [
                                {
                                    # Searching keyword in UserInput
                                    "match": {
                                        "UserInput": {
                                            "query": keyword,
                                            "operator": "and",
                                            "fuzziness": "AUTO"
                                        }
                                    }
                                }, {
                                    # Searching keyword in Response
                                    "match": {
                                        "Response": {
                                            "query": keyword,
                                            "operator": "and",
                                            "fuzziness": "AUTO"
                                            }
                                        }
                                    }
                                ]
                            }
                        }
            )

        # Run elasticsearch query
        results_gen = helpers.scan(self.elasticsearch, query=query, index=self.index, request_timeout=30)
        results = list(results_gen)
        
        # Appending results to dataframe
        for res in results:
            df = df.append(res['_source'], ignore_index=True)
        
        return df


# Method to get difference of two dataframes
def get_difference(data, success):

	unsuccess = data.merge(success, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
	unsuccess = unsuccess.drop('_merge', axis=1)

	return unsuccess


# Method to move email and AEBAS queries from unsuccessful to successful queries extracted from doubtful queries
def email_AEBAS(success, unsuccess):

	email = unsuccess[(unsuccess.UserInput.str.contains('abc@xyz.com', na=False, case=False)) & (unsuccess.Response.str.contains('Email', na=False, case=False))]
	
	aebas = ['attandence','attendance','attendence','finger','finger print','fingerprint','iris','mark',
			'marking','retina','thumb','ABAS','AEBAS','BAS','aadhaar','aadhar','addhar','adhaar','adhar',
			'bio matric','bio metric','bio metrices','bio metrics','bio-metric','biomatric','biometric',
			'biometrices','biometrics','biometrix']

	dataAEBAS = unsuccess[(unsuccess.UserInput.str.contains('|'.join(aebas), na=False, case=False)) & (unsuccess.Response.str.contains('AEBAS', na=False, case=False))]

	# Adding to successful
	success = pd.concat([success, email, dataAEBAS])
	success.to_csv('../Intermediate Log Data/DoubtToSuccessful.csv', index=False)

	# Removing from unsuccessful
	unsuccess = unsuccess.merge(email, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
	unsuccess = unsuccess.drop('_merge', axis=1)
	unsuccess = unsuccess.merge(dataAEBAS, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
	unsuccess = unsuccess.drop('_merge', axis=1)
	unsuccess.to_csv('../Intermediate Log Data/DoubtToUnsuccessful.csv', index=False)


# Method to add doubtful log data to successful and unsuccessful log data
def shift_doubt():

	# Reading data
	domSuc = pd.read_csv('../Intermediate Log Data/logsDomSuccess.csv')
	doubtSuc = pd.read_csv('../Intermediate Log Data/DoubtToSuccessful.csv')
	domUns = pd.read_csv('../Intermediate Log Data/logsDomUns.csv')
	doubtUns = pd.read_csv('../Intermediate Log Data/DoubtToUnsuccessful.csv')
	# Concatenating and writing successful data
	domSuccess = pd.concat([domSuc,doubtSuc])
	domSuccess.to_csv('../Intermediate Log Data/DomainSuccessful.csv', index=False)
	domSuccess.to_csv('../To Index/DomainSuccessful.csv', index=False)
	# Concatenating and writing unsuccessful data
	domUnsuccess = pd.concat([domUns,doubtUns])
	domUnsuccess.to_csv('../Intermediate Log Data/DomainUnsuccessful.csv', index=False)
	domUnsuccess.to_csv('../To Index/DomainUnsuccessful.csv', index=False)


if __name__ == '__main__':

	# Creating new dataframe to store results of keyword search on UserInput and Response fields
	df = pd.DataFrame(columns=['SessionID', 'Timestamp', 'IntentName','Event','UserInput','Response'])
	# Getting entity keywords data
	searchKey = sd.get_keywords()

	print('Segregating doubtful queries into successfully and unsuccessfully answered queries...')
	# Calling search method of KeywordSearch class on 'domdoubt' index
	doubtSuccess = KeywordSearch(index="domdoubt").search(keywords=searchKey, df=df)
	# Reading all doubtful data
	dataDoubt = pd.read_csv('../Intermediate Log Data/logsDomDoubt.csv')
	# Getting unsuccessful data from doubtful data
	doubtUnsuccess = get_difference(dataDoubt, doubtSuccess)
	# Calling email_AEBAS method
	email_AEBAS(doubtSuccess, doubtUnsuccess)

	print('Segregation successful.\n')

	print('Shifting this data to unsuccessful and successful log data...')
	# Moving segregated doubtful data to successful and unsuccessful data
	shift_doubt()
	print('Shifted data successfully.\nData stored in DomainUnsuccessful.csv and DomainSuccessful.csv')
