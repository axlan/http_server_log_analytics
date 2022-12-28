from dateutil import parser
from collections import defaultdict

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd

df = pd.read_csv('out/combined_logs.csv', parse_dates=['times'])

df['category'] = 'user'
df.loc[(df['c-device'] == 'Other') & (df['c-os'] == 'Other') & (df['c-os'] == 'Other'), 'category'] = 'bot'
df.loc[df['c-device'] == 'Spider', 'category'] = 'bot'

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
server = app.server

duration_days = [7, 30, 60, 180, 360]
durations_labels = [f'{d} Days' for d in duration_days]
durations = [{'label': l, 'value': v}
             for l, v in zip(durations_labels, duration_days)]
durations.append({'label': 'All', 'value': 0})

app.layout = dbc.Container([
    dbc.Label('Days History to Show'),
    dcc.Dropdown(
        id="dropdown_duration",
        options=durations,
        value=durations[-1]['value'],
        clearable=False
    ),
    dcc.RadioItems(id='data_type',options=['Requests', 'Unique Visitors'], value= 'Requests'),
    html.Br(),
    html.H1('Graphs'),
    dcc.Graph(id="request_graph"),
    dash_table.DataTable(
        id='visit_table',
        columns=[{"name": i, "id": i, 'presentation': 'markdown'} for i in ['Page', 'Visits']],
        sort_action="native",
        data=None,
    ),
])


@app.callback(
    Output("request_graph", "figure"),
    [Input("dropdown_duration", "value"),
    Input("data_type", "value")])
def update_request_graph(selected_days, data_type):
    if selected_days != 0:
        cutoff_date = df["times"].iloc[-1] - pd.Timedelta(days=selected_days)
        data = df[df['times'] > cutoff_date]
    else:
        data = df

    gp = data.groupby([data['times'].dt.date, 'category'])
    
    if data_type == 'Requests':
        counts = gp['c-ip'].count()
    else:
        counts = gp['c-ip'].nunique()

    counts = counts.reset_index(level=[1])

    fig = px.scatter(x=counts.index, y=counts['c-ip'], color=counts['category'])
    return fig


@app.callback(
    Output("visit_table", "data"),
    [Input("dropdown_duration", "value"),
    Input("data_type", "value")])
def update_visit_table(selected_days, data_type):
    if selected_days != 0:
        cutoff_date = df["times"].iloc[-1] - pd.Timedelta(days=selected_days)
        data = df[df['times'] > cutoff_date]
    else:
        data = df

    data = data[data['cs-uri-stem'].str.endswith('/')]
    gp = data.groupby(['cs-uri-stem'])
    
    if data_type == 'Requests':
        counts = gp['c-ip'].count()
    else:
        counts = gp['c-ip'].nunique()


    counts = counts.reset_index().sort_values('c-ip', ascending=False).head(20)

    dict_data = [{'Page':f"[{v['cs-uri-stem']}](https://www.robopenguins.com{v['cs-uri-stem']})",'Visits':v['c-ip']} for _, v in counts.iterrows()]

    return dict_data


if __name__ == "__main__":
    app.run_server(host='0.0.0.0', port=8282, debug=True)
