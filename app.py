'''
Author: Varun Chopra
Date: 09-06-2020

The following python code uses Dash framework by Plotly to create an interactive interface for results of the graphical analysis of Vani's log data.
Relevant explanations have been given in comments. 
Refer https://dash.plotly.com/dash-html-components for better understanding of Dash HTML Components
'''

# Importing Dash and Plotly dependencies
import dash
import dash_core_components as dcc
from datetime import datetime as dt
import dash_html_components as html
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from dash.dependencies import Input, Output
import pandas as pd
import dash_bootstrap_components as dbc
import dash.dependencies as dd

# For image transfer
from io import BytesIO

# For Wordcloud
from wordcloud import WordCloud
import base64

# Initializing dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

server = app.server


# Reading all relevant CSV files and converting the elements in Timestamp field to datetime objects
complete = pd.read_csv('Intermediate Log Data/AllLogsFinal.csv')
complete['Timestamp'] = pd.to_datetime(complete['Timestamp'])

gen = pd.read_csv('Intermediate Log Data/GeneralFinal.csv')
dom = pd.read_csv('Intermediate Log Data/DomainFinal.csv')

gen['Timestamp'] = pd.to_datetime(gen['Timestamp'])
dom['Timestamp'] = pd.to_datetime(dom['Timestamp'])

gensuc = pd.read_csv('Final Tasks Log Data/GeneralSuccessful.csv')
genuns = pd.read_csv('Final Tasks Log Data/GeneralUnsuccessful.csv')

gensuc['Timestamp'] = pd.to_datetime(gensuc['Timestamp'])
genuns['Timestamp'] = pd.to_datetime(genuns['Timestamp'])

domsuc = pd.read_csv('Intermediate Log Data/DomainSuccessful.csv')
domuns = pd.read_csv('Final Tasks Log Data/FilteredDomainUnsuccess.csv')

domsuc['Timestamp'] = pd.to_datetime(domsuc['Timestamp'])
domuns['Timestamp'] = pd.to_datetime(domuns['Timestamp'])

task3 = pd.read_csv('Final Tasks Log Data/FilteredTask3.csv')
task4 = pd.read_csv('Intermediate Log Data/Task3Neg.csv')

task3['Timestamp2'] = pd.to_datetime(task3['Timestamp2'])
task4['Timestamp2'] = pd.to_datetime(task4['Timestamp2'])


'''
----------------------------------------------------------------------------------------------------------------------------
                                                Section: HTML layout of web app
----------------------------------------------------------------------------------------------------------------------------             
'''

# HTML <title> tag
app.title = 'Chatbot Log Data Analysis'

# Creating layout of app
app.layout = html.Div([

    # Bootstrap container class
    dbc.Container(
    html.Div(style={'textAlign': 'center'}, children=[
    
    # HTML <br> tag
    html.Br(),
    html.Br(),

    # HTML <h1> tag
    html.H1(
        style={
              'font-size': '32px',
              'font-weight': 'bold',
              'text-transform': 'uppercase',
              'margin-bottom': '20px',
              'padding-bottom': '20px',
              'position': 'relative',
              'color': '#45505b'
        },

        # children=<content of tag>
        children='CHATBOT LOG DATA ANALYSIS'
    ),

    # HTML <p> tag
    html.P(
        children='The following sections of this web app present a graphical analysis of log data for VANI.'
    ),

    html.Br(),

    html.P(
    	children='Select a time period for analysis:'
    ),

    # For selection of date range
    dcc.DatePickerRange(
        id='date-picker-range',
        display_format='DD/MM/Y',
        min_date_allowed=dt(2019, 8, 25),
        max_date_allowed=dt(2020, 4, 29),
        initial_visible_month=dt(2020,4,29),
        start_date=dt(2019,8,26),
        end_date=dt(2020,4,28)
    ),

    # Embedding DatePickerRange in HTML
    html.Div(id='output-container-date-picker-range'),

    html.Br(),
    html.Hr(),
    html.Br(),

    html.H3(
        children='SEGREGATION ON THE BASIS OF GENERAL AND DOMAIN SPECIFIC QUERIES'
    ),

    # Graph for segregation of queries into general and domain specific categories
    dcc.Graph(id='dom-gen'),

    html.Hr(),
    html.Br(),

    html.H3(
        children='MOST FREQUENT INTENT NAMES IN COMPLETE LOG DATA'
    ),

    # Graph for intent names in complete log data
    dcc.Graph(id='complete-intent'),

    html.Hr(),
    html.Br(),

    html.H3(
        children='MOST FREQUENT EVENTS IN COMPLETE LOG DATA'
    ),

    # Graph for events in complete log data
    dcc.Graph(id='complete-event'),
    
    html.Hr(),
    html.Br(),

    html.H3(
        children='MOST FREQUENT INTENT NAMES IN GENERAL LOG DATA'
    ),

    # Graph for intent names in general log data
    dcc.Graph(id='gen-intent'),

    html.Hr(),
    html.Br(),

    html.H3(
        children='MOST FREQUENT EVENTS IN GENERAL LOG DATA'
    ),

    # Graph for events in general log data
    dcc.Graph(id='gen-event'),

    html.Hr(),
    html.Br(),

    html.H3(
        children='MOST FREQUENT TERMS IN USER QUERIES FOR GENERAL LOG DATA'
    ),

    html.Br(),
    # Wordcloud for User Input in general log data
    html.Img(id="image_wc"),

    html.Br(),
    html.Br(),
    html.Hr(),
    html.Br(),

    html.H3(
        children='SEGREGATION OF GENERAL LOG DATA BASED ON SUCCESSFULLY AND UNSUCCESSFULLY ANSWERED QUERIES'
    ),

    # Graph for successfully and unsuccessfully answered queries in general log data
    dcc.Graph(id='gen-div'),

    html.Hr(),
    html.Br(),

    html.H3(
        children='MOST FREQUENT INTENT NAMES IN DOMAIN SPECIFIC LOG DATA'
    ),

    # Graph for intent names in domain specific log data
    dcc.Graph(id='dom-intent'),

    html.Hr(),
    html.Br(),

    html.H3(
        children='MOST FREQUENT EVENTS IN DOMAIN SPECIFIC LOG DATA'
    ),

    # Graph for events in domain specific log data
    dcc.Graph(id='dom-event'),

    html.Hr(),
    html.Br(),

    html.H3(
        children='MOST FREQUENT TERMS IN USER QUERIES FOR DOMAIN SPECIFIC LOG DATA'
    ),

    html.Br(),
    # Wordcloud for User Input in domain specific log data
    html.Img(id="image_wc2"),

    html.Br(),
    html.Br(),
    html.Hr(),
    html.Br(),

    html.H3(
        children='SEGREGATION OF DOMAIN SPECIFIC LOG DATA BASED ON SUCCESSFULLY AND UNSUCCESSFULLY ANSWERED QUERIES'
    ),

    dcc.Graph(id='dom-div'),

    html.Hr(),
    html.Br(),

    html.H3(
        children='IMPROVEMENT IN RECOGNITION OF DOMAIN SPECIFIC QUERIES'
    ),

    # Graph for successfully and unsuccessfully answered queries in domain specific log data
    dcc.Graph(id='improvement'),

    html.Hr(),
    html.Br(),

    html.H3(
        children='MONTHLY TREND FOR IMPROVEMENT IN RECOGNITION OF DOMAIN SPECIFIC QUERIES'
    ),

    html.Br(),

    html.P(
        children='The following bar graph represents the percentage of monthly queries that could not be answered at first but could be answered later, with respect to those queries which could not be answered even later on.'
    ),

    # Graph for improvement on a monthly basis
    dcc.Graph(id='improvement-monthly'),

])
),

# HTML <footer> tag
html.Footer(
        style={
              'background': '#f7f8f9',
              'color': '#45505b',
              'font-size': '14px',
              'text-align': 'center',
              'padding': '30px 0'
        },
        children=html.Div(
            className='container',
            children=[
                html.H3(
                    style={
                          'font-size': '36px',
                          'font-weight': '700',
                          'position': 'relative',
                          'font-family': '"Poppins", sans-serif',
                          'padding': '0',
                          'margin': '0 0 15px 0',
                    },
                    children='CHATBOT LOG DATA ANALYSIS'
                ),
                html.Div(
                    style={
                        'margin': '0 0 5px 0'
                    },
                    className='copyright',
                    children='Copyright 2020 AIRD, National Informatics Centre'
                )
            ]
        )
    )
])

'''
----------------------------------------------------------------------------------------------------------------------------
                                                Section: Normal python methods
----------------------------------------------------------------------------------------------------------------------------             
'''

# Method to create wordcloud image
def plot_wordcloud(data):
    d = {a: x for a, x in data.values}
    wc = WordCloud(background_color='white', width=480, height=360)
    wc.fit_words(d)
    return wc.to_image()


'''
----------------------------------------------------------------------------------------------------------------------------
                                                Section: Dash callback methods to build graphs
----------------------------------------------------------------------------------------------------------------------------   

The following methods take start_date and end_date inputs from the web page and return graphs made using Plotly         
'''

# Segregation into general and domain specific queries
@app.callback(
    # Output graph
    Output('dom-gen', 'figure'),
    # Input start_date and end_date
    [Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def gen_dom(start_date, end_date):

    # Getting global dataframes
    global gen, dom

    colors = ['lightgreen', 'green']

    # Trimming dataframe to required time period
    gen_trim = gen[(gen.Timestamp >= start_date) & (gen.Timestamp <= end_date)]
    dom_trim = dom[(dom.Timestamp >= start_date) & (dom.Timestamp <= end_date)]

    labels = ['General Queries', 'Domain Specific Queries']
    values = [gen_trim.shape[0], dom_trim.shape[0]]

    fig = go.Figure(go.Pie(
#     title='MOST FREQUENT EVENTS IN COMPLETE LOG DATA',
        labels=labels, 
        values=values
            )
        )

    fig.update_traces(hoverinfo='label+value', textinfo='percent', textfont_size=14, marker=dict(colors=colors, line=dict(color='#000000', width=1)))

    return fig


# Intent Names in complete log data
@app.callback(
    Output('complete-intent', 'figure'),
    [Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def complete_intent(start_date, end_date):

    global complete

    complete_trim = complete[(complete.Timestamp >= start_date) & (complete.Timestamp <= end_date)]

    # Getting 10 most frequent intent names
    n = 10
    complete_intent = dict(complete_trim['IntentName'].value_counts()[:n])

    fig = go.Figure(go.Bar(
    x=list(complete_intent.values()), 
    y=list(complete_intent.keys()),
    orientation='h'
    ))
    fig.update_layout(
        # title='MOST FREQUENT INTENT NAMES IN COMPLETE LOG DATA FROM {} TO {}'.format(start_date[:10], end_date[:10]),
        xaxis=dict(
            title='Count'
        ),
        yaxis=dict(
            title='Intent Names',
            autorange="reversed"
        )
    )

    return fig


# Events in complete log data
@app.callback(
    Output('complete-event', 'figure'),
    [Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def complete_event(start_date, end_date):

    global complete

    complete_trim = complete[(complete.Timestamp >= start_date) & (complete.Timestamp <= end_date)]

    n = 10
    complete_event = dict(complete_trim['Event'].value_counts()[:n])
    colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']

    fig = go.Figure(go.Pie(
        # title='MOST FREQUENT EVENTS IN COMPLETE LOG DATA',
        labels=list(complete_event.keys()), 
        values=list(complete_event.values()),
        hole=0.5
        )
    )
    fig.update_traces(hoverinfo='label+value', textinfo='percent', textfont_size=14, marker=dict(colors=colors, line=dict(color='#000000', width=1)))

    return fig


# Intents in general log data
@app.callback(
    Output('gen-intent', 'figure'),
    [Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def gen_intent(start_date, end_date):

    global gen

    gen_trim = gen[(gen.Timestamp >= start_date) & (gen.Timestamp <= end_date)]

    n = 10
    gen_intent = dict(gen_trim['IntentName'].value_counts()[:n])

    fig = go.Figure(go.Bar(
    x=list(gen_intent.values()), 
    y=list(gen_intent.keys()),
    orientation='h'
    ))
    fig.update_layout(
        # title='MOST FREQUENT INTENT NAMES IN COMPLETE LOG DATA FROM {} TO {}'.format(start_date[:10], end_date[:10]),
        xaxis=dict(
            title='Count'
        ),
        yaxis=dict(
            title='Intent Names',
            autorange="reversed"
        )
    )

    return fig


# Events in general log data
@app.callback(
    Output('gen-event', 'figure'),
    [Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def gen_event(start_date, end_date):

    global gen

    gen_trim = gen[(gen.Timestamp >= start_date) & (gen.Timestamp <= end_date)]

    n = 10
    gen_event = dict(gen_trim['Event'].value_counts()[:n])
    colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']

    fig = go.Figure(go.Pie(
        # title='MOST FREQUENT EVENTS IN COMPLETE LOG DATA',
        labels=list(gen_event.keys()), 
        values=list(gen_event.values()),
        hole=0.5
        )
    )
    fig.update_traces(hoverinfo='label+value', textinfo='percent', textfont_size=14, marker=dict(colors=colors, line=dict(color='#000000', width=1)))

    return fig


# Wordcloud for general data user input
@app.callback(
    Output('image_wc', 'src'), 
    [Input('image_wc', 'id'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def make_image(b, start_date, end_date):

    global gen

    gen_trim = gen[(gen.Timestamp >= start_date) & (gen.Timestamp <= end_date)]

    n = 50
    # Creating dataframe with 50 most frequently used words and their frequency
    df = pd.DataFrame(columns=['Word','Value'])
    gen_query = dict(gen['UserInput'].value_counts()[:n])

    df["Word"] = gen_query.keys()
    df['Value'] = gen_query.values()
    
    # Returning a png image of wordcloud generated by plot_wordcloud() method and embedding it into web page
    img = BytesIO()
    plot_wordcloud(data=df).save(img, format='PNG')
    return 'data:image/png;base64,{}'.format(base64.b64encode(img.getvalue()).decode())


# Successful and unsuccesfull general log data
@app.callback(
    Output('gen-div', 'figure'),
    [Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def gen_div(start_date, end_date):

    global gensuc, genuns

    colors = ['lightgreen', 'orange']

    gensuc_trim = gensuc[(gensuc.Timestamp >= start_date) & (gensuc.Timestamp <= end_date)]
    genuns_trim = genuns[(genuns.Timestamp >= start_date) & (genuns.Timestamp <= end_date)]

    labels = ['Successfully answered', 'Could not be answered']
    # shape[0] gives the number of rows in dataframe
    values = [gensuc_trim.shape[0], genuns_trim.shape[0]]

    fig = go.Figure(go.Pie(
#     title='MOST FREQUENT EVENTS IN COMPLETE LOG DATA',
        labels=labels, 
        values=values
            )
        )

    fig.update_traces(hoverinfo='label+value', textinfo='percent', textfont_size=14, marker=dict(colors=colors, line=dict(color='#000000', width=1)))

    return fig


# Intent names in domain specific data
@app.callback(
    Output('dom-intent', 'figure'),
    [Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def dom_intent(start_date, end_date):

    global dom

    dom_trim = dom[(dom.Timestamp >= start_date) & (dom.Timestamp <= end_date)]

    n = 10
    dom_intent = dict(dom_trim['IntentName'].value_counts()[:n])

    fig = go.Figure(go.Bar(
    x=list(dom_intent.values()), 
    y=list(dom_intent.keys()),
    orientation='h'
    ))
    fig.update_layout(
        # title='MOST FREQUENT INTENT NAMES IN COMPLETE LOG DATA FROM {} TO {}'.format(start_date[:10], end_date[:10]),
        xaxis=dict(
            title='Count'
        ),
        yaxis=dict(
            title='Intent Names',
            autorange="reversed"
        )
    )

    return fig


# Events in domain specific data
@app.callback(
    Output('dom-event', 'figure'),
    [Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def dom_event(start_date, end_date):

    global dom

    dom_trim = dom[(dom.Timestamp >= start_date) & (dom.Timestamp <= end_date)]

    n = 10
    dom_event = dict(dom_trim['Event'].value_counts()[:n])
    colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']

    fig = go.Figure(go.Pie(
        # title='MOST FREQUENT EVENTS IN COMPLETE LOG DATA',
        labels=list(dom_event.keys()), 
        values=list(dom_event.values()),
        hole=0.5
        )
    )
    fig.update_traces(hoverinfo='label+value', textinfo='percent', textfont_size=14, marker=dict(colors=colors, line=dict(color='#000000', width=1)))

    return fig


# Wordcloud for user input in domain specific data
@app.callback(
    Output('image_wc2', 'src'), 
    [Input('image_wc2', 'id'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def make_image(b, start_date, end_date):

    global dom

    dom_trim = dom[(dom.Timestamp >= start_date) & (dom.Timestamp <= end_date)]

    n = 50
    df = pd.DataFrame(columns=['Word','Value'])
    dom_query = dict(dom['UserInput'].value_counts()[:n])

    df["Word"] = dom_query.keys()
    df['Value'] = dom_query.values()
    
    img = BytesIO()
    plot_wordcloud(data=df).save(img, format='PNG')
    return 'data:image/png;base64,{}'.format(base64.b64encode(img.getvalue()).decode())


# Successful and unsuccessful domain specific queries
@app.callback(
    Output('dom-div', 'figure'),
    [Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def dom_div(start_date, end_date):

    global domsuc, domuns

    colors = ['lightgreen', 'orange']

    domsuc_trim = domsuc[(domsuc.Timestamp >= start_date) & (domsuc.Timestamp <= end_date)]
    domuns_trim = domuns[(domuns.Timestamp >= start_date) & (domuns.Timestamp <= end_date)]

    labels = ['Successfully answered', 'Could not be answered']
    values = [domsuc_trim.shape[0], domuns_trim.shape[0]]

    fig = go.Figure(go.Pie(
#     title='MOST FREQUENT EVENTS IN COMPLETE LOG DATA',
        labels=labels, 
        values=values
            )
        )

    fig.update_traces(hoverinfo='label+value', textinfo='percent', textfont_size=14, marker=dict(colors=colors, line=dict(color='#000000', width=1)))

    return fig


# Graph for improvement in recognition of domain specific queries
@app.callback(
    Output('improvement', 'figure'),
    [Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def improv(start_date, end_date):

    global task3, task4

    colors = ['lightgreen', 'lightred']

    task3_trim = task3[(task3.Timestamp2 >= start_date) & (task3.Timestamp2 <= end_date)]
    task4_trim = task4[(task4.Timestamp2 >= start_date) & (task4.Timestamp2 <= end_date)]

    # Getting unique queries that could not be answered at first but were successfully answered later on
    uni_task3 = task3_trim['UserInput2'].unique()
    # Getting unique queries that could not be answered at first and could not be successfully answered even later on
    uni_task4 = task4_trim['UserInput2'].unique()

    labels = ['Queries that could not be answered at first but were successfully answered later on', 'Queries that could not be answered even later on']
    values = [uni_task3.shape[0], uni_task4.shape[0]]

    fig = go.Figure(go.Pie(
#     title='MOST FREQUENT EVENTS IN COMPLETE LOG DATA',
        labels=labels, 
        values=values
            )
        )

    fig.update_traces(hoverinfo='label+value', textinfo='percent', textfont_size=14, marker=dict(colors=colors, line=dict(color='#000000', width=1)))

    return fig


# Graph for monthly trend in improvement 
@app.callback(
    Output('improvement-monthly', 'figure'),
    [Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')]
)
def improv_monthly(start_date, end_date):

    # Here task4 stands for the queries that could not be answered even on a later try
    global task3, task4

    task3_trim = task3[(task3.Timestamp2 >= start_date) & (task3.Timestamp2 <= end_date)]
    task4_trim = task4[(task4.Timestamp2 >= start_date) & (task4.Timestamp2 <= end_date)]

    # Dropping left side of dataframes
    uni_task3 = task3_trim.drop(['SessionID','Timestamp','IntentName','Event','UserInput','Response'],axis=1)
    uni_task4 = task4_trim.drop(['SessionID','Timestamp','IntentName','Event','UserInput','Response'],axis=1)

    # Grouping data by month in Timestamp2 field and getting count of data in each month's group
    gp_task3 = uni_task3['Timestamp2'].groupby(uni_task3.Timestamp2.dt.to_period("M")).agg('count')
    gp_task4 = uni_task4['Timestamp2'].groupby(uni_task4.Timestamp2.dt.to_period("M")).agg('count')

    # Getting percentage of unsuccessful queries that could be answered later on with respect to those that could not be answered successfully even later on
    percent = gp_task3 / (gp_task3+gp_task4)*100

    # Getting labels for graph by changing format to Mon-YYYY
    labels = percent.index.strftime('%b-%Y')

    # Creating graph
    fig = go.Figure(go.Bar(
        x=percent,
        y=labels,
        marker_color='rgb(66, 75, 245)',
        orientation='h'
    ))

    fig.update_layout(
        xaxis=dict(
            title='Percentage'
        ),
        yaxis=dict(
            title='Month',
            autorange="reversed"
        ),
        bargap=0.6
    )

    return fig


'''
----------------------------------------------------------------------------------------------------------------------------
                                    Section: Main method to start the app on localhost:8050
----------------------------------------------------------------------------------------------------------------------------             
'''

if __name__ == '__main__':

    app.run_server(debug=True)


