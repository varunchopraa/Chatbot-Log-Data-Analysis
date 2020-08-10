# Chatbot Log Data Analysis

This repo consists of the scripts used in the analysis of chatbot log data for my internship at NIC, Delhi from May 2020 to June 2020. The logs are extracted from their directory structure and then segregated into the following four categories:

- General queries that could not be answered by the chatbot.
- General queries that were successfully answered by the chatbot.
- Domain specific queries that could not be answered at first but were successfully answered later on.
- Domain specific queries that could not be answered.

The analysis and segregation were done using Pandas along with Elasticsearch, Kibana and Excelastic for indexing CSV files.

The repo also contains _app.py_ which is responsible for starting the visualisation dashboard for the results of the analysis on localhost. The dashboard has been developed using Plotly's Dash framework.
