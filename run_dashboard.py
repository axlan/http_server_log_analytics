import webbrowser
from threading import Timer

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd

TABLE_ROWS = 20

df = pd.read_csv('out/combined_logs.csv', parse_dates=['times'])

df['category'] = 'user'
df.loc[(df['c-device'] == 'Other') & (df['c-os'] == 'Other')
       & (df['c-os'] == 'Other'), 'category'] = 'bot'
df.loc[df['c-device'] == 'Spider', 'category'] = 'bot'

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
server = app.server

duration_days = [7, 30, 60, 180, 360]
durations_labels = [f'{d} Days' for d in duration_days]
durations = [{'label': l, 'value': v}
             for l, v in zip(durations_labels, duration_days)]
durations.append({'label': 'All', 'value': 0})

app.layout = dbc.Container([
    dbc.Row(
        [
            dbc.Label('Days History to Show',  width=2),
            dbc.Col(
                dcc.Dropdown(
                    id="dropdown_duration",
                    options=durations,
                    value=durations[-1]['value'],
                    clearable=False
                ),
                width=10,
            ),
        ],
        className="mb-3",
    ),
    dbc.Row(
        [
            dbc.Label('Which Data to Display',  width=2),
            dbc.Col(
                dbc.RadioItems(id='data_type', options=[{'label': v, 'value': v}
                                                        for v in ['Requests', 'Unique Visitors']], value='Requests'),
                width=10,
            ),
        ],
        className="mb-3",
    ),
    html.Br(),
    dcc.Graph(id="request_graph"),
    dash_table.DataTable(
        id='visit_table',
        columns=[{"name": i, "id": i, 'presentation': 'markdown'}
                 for i in ['Page', 'Human Visits', 'Bot Visits']],
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

    fig = px.scatter(
        x=counts.index, y=counts['c-ip'], color=counts['category'], labels={
            "x": "Date",
            "y": f"{data_type}/day"
        },
        title=f"{data_type} Trends")

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
    gp = data.groupby(['cs-uri-stem', 'category'])

    if data_type == 'Requests':
        counts = gp['c-ip'].count()
    else:
        counts = gp['c-ip'].nunique()

    counts = counts.reset_index([1]).sort_values('c-ip', ascending=False)

    idx_bot = counts['category'] == 'bot'
    idx_not_bot = counts['category'] != 'bot'

    cat_counts = counts[idx_not_bot].copy()

    cat_counts['Bot Visits'] = counts[idx_bot]['c-ip']

    cat_counts.dropna(inplace=True)
    cat_counts['Bot Visits'] = cat_counts['Bot Visits'].astype(int)
    cat_counts = cat_counts.head(20)

    dict_data = [{'Page': f"[{i}](https://www.robopenguins.com{i})",
                  'Human Visits': v['c-ip'], 'Bot Visits': v['Bot Visits']} for i, v in cat_counts.iterrows()]

    return dict_data


def open_browser():
    webbrowser.open_new("http://localhost:{}".format(8282))


if __name__ == '__main__':
    Timer(1, open_browser).start()
    app.run_server(host='0.0.0.0', port=8282, debug=True)
