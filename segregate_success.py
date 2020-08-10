'''
Author: Varun Chopra
Date: 04-06-2020

This Py file is responsible for segregating domain specific and general log data into the following three categories:
	1. Unsuccessful
	2. Doubtful
	3. Successful
Pandas phrase matching is used on the Response field to achieve this.
Two methods are defined below, with their purposes described in comments.
Products of this script are:
	1. logsDomUns.csv
	2. GeneralUnsuccessful.csv
	3. logsDomDoubt.csv
	4. GeneralDoubt.csv
	5. logsDomSuccess.csv
	6. GeneralSuccessful.csv
'''

import pandas as pd
import finalise_segregation


# Method to get unsuccessful and doubtful log data
def segregate_success(dom, gen, responses):

	dom = dom[dom.Response.str.contains('|'.join(responses), na=False, case=False)]
	gen = gen[gen.Response.str.contains('|'.join(responses), na=False, case=False)]

	return [dom, gen]


# Method to get successful log data by taking the data that is neither doubtful nor unsuccessful
def get_successful(data, uns, doubt):

	# Concatenate unsuccessful and doubtful log data
	notSuccess = pd.concat([uns,doubt])
	# Take difference of total data and sum of unsuccessful and doubtful log data
	success = data.merge(notSuccess, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
	success = success.drop('_merge', axis=1)

	return success


if __name__ == '__main__':

	# Calling finalise_segregation's main method
	finalise_segregation.main()

	# Reading set of unsuccessful responses from unsuccessful.txt
	with open('../unsuccessful.txt') as f:
	    unsuccess = f.readlines()
	# Removing \n from end of a response
	unsuccess = [x.strip() for x in unsuccess] 

	# Reading domain specific and general data
	dataDom = pd.read_csv('../Intermediate Log Data/DomainFinal.csv')
	dataGen = pd.read_csv('../Intermediate Log Data/GeneralFinal.csv')

	print('Segregating unsuccessful queries...')
	# Getting unsuccessfully answered data
	res = segregate_success(dataDom, dataGen, unsuccess)
	domUns = res[0]
	genUns = res[1]
	# Writing to CSV files
	domUns.to_csv('../Intermediate Log Data/logsDomUns.csv', index=False)
	genUns.to_csv('../Final Tasks Log Data/GeneralUnsuccessful.csv', index=False)

	print('Segregation successful. \nData stored in logsDomUns.csv and GeneralUnsuccessful.csv.\n')

	# Reading set of doubtful responses from doubtful.txt
	with open('../doubtful.txt') as f:
	    doubtful = f.readlines()
	# Removing \n from end of a response
	doubtful = [x.strip() for x in doubtful] 

	print('Segregating doubtful queries...')
	# Getting doubtful data
	res = segregate_success(dataDom, dataGen, doubtful)
	domDoubt = res[0]
	genDoubt = res[1]
	# Writing to CSV files
	domDoubt.to_csv('../Intermediate Log Data/logsDomDoubt.csv', index=False)
	domDoubt.to_csv('../To Index/logsDomDoubt.csv', index=False)
	genDoubt.to_csv('../Intermediate Log Data/GeneralDoubt.csv', index=False)

	print('Segregation successful. \nData stored in logsDomDoubt.csv and GeneralDoubt.csv.\n')

	
	print('Segregating successful queries...')
	# Getting successfully answered data
	domSuccess = get_successful(dataDom, domUns, domDoubt)
	genSuccess = get_successful(dataGen, genUns, genDoubt)
	# Writing to CSV files
	domSuccess.to_csv('../Intermediate Log Data/logsDomSuccess.csv', index=False)
	genSuccess.to_csv('../Final Tasks Log Data/GeneralSuccessful.csv', index=False)
	print('Segregation successful. \nData stored in logsDomSuccess.csv and GeneralSuccessful.csv.\n')

