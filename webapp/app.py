import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from cassandra_connection import FetchData
import plotly.graph_objs as go

Borough = ('BTC-USD','ETH-USD','LTC-USD','BCH-USD','ETH-BTC', "LTC-BTC", "BCH-USD")


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div([
    html.H1(children='DeCrypto'),
    dcc.Graph(id='graph'),
    dcc.Dropdown(id='currency',
                 options=[{'label': p, 'value': p} for p in Borough],
                 multi=False, value=''
                 ),
    
    dcc.Interval(
        id='graph-update',
        interval=10 * 1000
    ),
])

@app.callback(
    Output(component_id='graph', component_property='figure'),
    [Input(component_id='currency', component_property='value')]
)
def update_output_div(input_value):
    cass_conn = FetchData()
    session = cass_conn.start_connection()
    stmnt = cass_conn.prepare_query(session)
    df = cass_conn.get_data(stmnt, session, input_value)
    
    return {
    'data': [
        go.Bar(
            x=df['time'].values,
            y=((df['bids'].values / df['asks'].values) -1) * 100,
            text=df['market'].values,
            marker=dict(
                color='rgb(158,202,225)',
                line=dict(
                    color='rgb(8,48,107)',
                    width=1.5,
                )),
            opacity=0.6)
    ],
    'layout':
    go.Layout(title='DeCrypto')
}


if __name__ == '__main__':
    app.run_server(debug=True)