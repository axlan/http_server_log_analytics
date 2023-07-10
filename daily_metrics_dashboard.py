import webbrowser
from threading import Timer

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

TABLE_ROWS = 20

df = pd.read_feather('out/daily_metrics.feather')

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
        cutoff_date = df["date"].iloc[-1] - pd.Timedelta(days=selected_days)
        data = df[df['date'] > cutoff_date]
    else:
        data = df

    
    gp = data.groupby([data['date'].dt.date])

    key_type = '_total_requests' if data_type == 'Requests' else '_unique_requests'

    human_counts = gp['human' + key_type].sum()
    bot_counts = gp['bot' + key_type].sum()

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=human_counts.index, y=human_counts,
                        mode='lines+markers',
                        name='human'))
    fig.add_trace(go.Scatter(x=bot_counts.index, y=bot_counts,
                        mode='lines+markers',
                        name='bot'))
    fig.update_layout(
    title=f"{data_type} Trends",
    xaxis_title="Date",
    yaxis_title=f"{data_type}/day",
    )
    return fig


@app.callback(
    Output("visit_table", "data"),
    [Input("dropdown_duration", "value"),
     Input("data_type", "value")])
def update_visit_table(selected_days, data_type):
    if selected_days != 0:
        cutoff_date = df["date"].iloc[-1] - pd.Timedelta(days=selected_days)
        data = df[df['date'] > cutoff_date]
    else:
        data = df

    gp = data.groupby(['page'])


    key_type = '_total_requests' if data_type == 'Requests' else '_unique_requests'

    gp = gp.sum(numeric_only=True).sort_values('human' + key_type, ascending=False).head(20)

    dict_data = [{'Page': f"[{i}](https://www.robopenguins.com{i})",
                  'Human Visits': v['human' + key_type], 'Bot Visits': v['bot' + key_type]} for i, v in gp.iterrows()]

    return dict_data


def open_browser():
    webbrowser.open_new("http://localhost:{}".format(8282))


if __name__ == '__main__':
    Timer(1, open_browser).start()
    app.run_server(host='0.0.0.0', port=8282, debug=True)
