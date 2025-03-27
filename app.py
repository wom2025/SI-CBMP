import dash
from dash import dcc, html, Input, Output, State
import pandas as pd
import io
from datetime import datetime, timedelta
import aiohttp
import asyncio
import plotly.graph_objects as go
import plotly.io as pio

app = dash.Dash(__name__)

# Helpers
async def fetch_csv(dataset, date_str):
    base_url = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/"
    url = f"{base_url}{dataset}/exports/csv?lang=nl&refine=datetime%3A%22{date_str}%22&timezone=Europe%2FBrussels&use_labels=true&delimiter=%3B"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            text = await response.text()
            df = pd.read_csv(io.StringIO(text), delimiter=';')
            df.columns = df.columns.str.strip()
            return df

def get_column(df, possible):
    for name in possible:
        for col in df.columns:
            if name.lower() in col.lower():
                return col
    return None

def process_ods133(df):
    time_col = get_column(df, ['datetime', 'tijdstip'])
    df[time_col] = pd.to_datetime(df[time_col], utc=True)
    df['Datetime'] = df[time_col].dt.tz_convert('Europe/Brussels')
    df['SI'] = pd.to_numeric(df[get_column(df, ['system imbalance'])], errors='coerce')
    df['ACE'] = pd.to_numeric(df[get_column(df, ['area control error', 'ACE'])], errors='coerce')
    df['MIP'] = pd.to_numeric(df[get_column(df, ['marginal incremental'])], errors='coerce')
    df['MDP'] = pd.to_numeric(df[get_column(df, ['marginal decremental'])], errors='coerce')
    return df[['Datetime', 'SI', 'ACE', 'MIP', 'MDP']]

def process_ods134(df):
    time_col = get_column(df, ['datetime', 'tijdstip'])
    df[time_col] = pd.to_datetime(df[time_col], utc=True)
    df['Datetime'] = df[time_col].dt.tz_convert('Europe/Brussels') + pd.Timedelta(minutes=15)
    df['Imbalance'] = pd.to_numeric(df[get_column(df, ['imbalance price'])], errors='coerce')
    return df[['Datetime', 'Imbalance']]

def process_cbmp(date_str):
    url = f"https://api.transnetbw.de/picasso-cbmp/csv?date={date_str}&lang=de"
    try:
        df = pd.read_csv(url, delimiter=';')
        df['Zeit (ISO 8601)'] = pd.to_datetime(df['Zeit (ISO 8601)'], errors='coerce')
        if df['Zeit (ISO 8601)'].dt.tz is None:
            df['Datetime'] = df['Zeit (ISO 8601)'].dt.tz_localize('UTC').dt.tz_convert('Europe/Brussels')
        else:
            df['Datetime'] = df['Zeit (ISO 8601)'].dt.tz_convert('Europe/Brussels')
        df['ELIA_POS'] = pd.to_numeric(df['ELIA_POS'], errors='coerce')
        df['ELIA_NEG'] = pd.to_numeric(df['ELIA_NEG'], errors='coerce')
        return df[['Datetime', 'ELIA_POS', 'ELIA_NEG']]
    except Exception as e:
        print(f"Fout bij ophalen van CBMP-data: {e}")
        return pd.DataFrame(columns=['Datetime', 'ELIA_POS', 'ELIA_NEG'])

async def fetch_all_csv(date_str):
    df_133, df_134 = await asyncio.gather(
        fetch_csv('ods133', date_str),
        fetch_csv('ods134', date_str)
    )
    return df_133, df_134

# Layout
app.layout = html.Div([
    html.H1("SI & ACE + CBMP / Prijzen Dashboard"),
    dcc.DatePickerSingle(
        id='date-picker',
        date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        display_format='YYYY-MM-DD'
    ),
    html.Div([
        html.Label("Hulplijn 1 (y-waarde):"),
        dcc.Input(id='line1', type='number', value=200, step=1),
        html.Label("Hulplijn 2 (y-waarde):"),
        dcc.Input(id='line2', type='number', value=-200, step=1)
    ], style={'marginBottom': 20, 'marginTop': 20}),
    html.H2("System Imbalance (SI) & ACE"),
    dcc.Loading([
        dcc.Graph(id='si-ace-graph'),
        html.Button("Download SI + ACE als PNG", id="download-si-ace", n_clicks=0),
        dcc.Download(id="download-si-ace-image")
    ]),
    html.H2("CBMP + Prijzen (ods133 + 134)"),
    dcc.Loading([
        dcc.Graph(id='cbmp-graph'),
        html.Button("Download CBMP + Prijzen als PNG", id="download-cbmp", n_clicks=0),
        dcc.Download(id="download-cbmp-image")
    ])
])

@app.callback(
    Output('si-ace-graph', 'figure'),
    Output('cbmp-graph', 'figure'),
    Input('date-picker', 'date'),
    Input('line1', 'value'),
    Input('line2', 'value')
)
def update_graphs(date_str, line1, line2):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    df_133, df_134 = loop.run_until_complete(fetch_all_csv(date_str))
    loop.close()

    cbmp_df = process_cbmp(date_str)

    if not df_133.empty:
        df1 = process_ods133(df_133)
    else:
        df1 = pd.DataFrame(columns=['Datetime', 'SI', 'ACE', 'MIP', 'MDP'])

    if not df_134.empty:
        df2 = process_ods134(df_134)
    else:
        df2 = pd.DataFrame(columns=['Datetime', 'Imbalance'])

    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(x=df1['Datetime'], y=df1['SI'], mode='lines', name='SI (MW)', line=dict(color='gray')))
    fig1.add_trace(go.Scatter(x=df1['Datetime'], y=df1['ACE'], mode='lines', name='ACE (MW)', line=dict(color='orange')))
    fig1.update_layout(title='System Imbalance (SI) & ACE', hovermode='x unified',
                       xaxis=dict(showgrid=True, dtick=900000, matches='x'),
                       yaxis=dict(showgrid=True), height=500)

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=df1['Datetime'], y=df1['MIP'], mode='lines', name='MIP'))
    fig2.add_trace(go.Scatter(x=df1['Datetime'], y=df1['MDP'], mode='lines', name='MDP'))
    fig2.add_trace(go.Scatter(x=df2['Datetime'], y=df2['Imbalance'], mode='lines', name='Imbalance Price'))
    if not cbmp_df.empty:
        fig2.add_trace(go.Scatter(x=cbmp_df['Datetime'], y=cbmp_df['ELIA_POS'], mode='lines', name='ELIA_POS'))
        fig2.add_trace(go.Scatter(x=cbmp_df['Datetime'], y=cbmp_df['ELIA_NEG'], mode='lines', name='ELIA_NEG'))
        if line1 is not None:
            fig2.add_hline(y=line1, line_color='lightblue', line_dash='dot')
        if line2 is not None:
            fig2.add_hline(y=line2, line_color='lightblue', line_dash='dot')
    fig2.update_layout(title='CBMP / MIP / MDP / Imbalance', hovermode='x unified',
                       xaxis=dict(showgrid=True, dtick=900000, matches='x'),
                       yaxis=dict(showgrid=True), height=700)

    return fig1, fig2

@app.callback(
    Output("download-si-ace-image", "data"),
    Input("download-si-ace", "n_clicks"),
    Input("si-ace-graph", "figure"),
    prevent_initial_call=True
)
def download_si_figure(n, fig):
    fig_obj = go.Figure(fig)
    return dcc.send_bytes(lambda x: pio.write_image(fig_obj, x, format='png'), filename="SI_ACE.png")

@app.callback(
    Output("download-cbmp-image", "data"),
    Input("download-cbmp", "n_clicks"),
    Input("cbmp-graph", "figure"),
    prevent_initial_call=True
)
def download_cbmp_figure(n, fig):
    fig_obj = go.Figure(fig)
    return dcc.send_bytes(lambda x: pio.write_image(fig_obj, x, format='png'), filename="CBMP_Pricing.png")
import os

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
