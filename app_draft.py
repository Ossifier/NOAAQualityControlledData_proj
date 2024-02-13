from dash import Dash, html, dash_table, dcc
import pandas as pd
import plotly.express as px
from dash_extensions import DeferScript

app = Dash(__name__, assets_folder='app_draft_assets')

# Import Binned Data:
df_ak_2020 = pd.read_csv('NOAA Quality Controlled Datasets_csv/CRND20240209/2020/CRND0103-2020-AK_Aleknagik_1_NNE.csv')
df_ak_2021 = pd.read_csv('NOAA Quality Controlled Datasets_csv/CRND20240209/2021/CRND0103-2021-AK_Aleknagik_1_NNE.csv')
df_ak_2022 = pd.read_csv('NOAA Quality Controlled Datasets_csv/CRND20240209/2022/CRND0103-2022-AK_Aleknagik_1_NNE.csv')
df_ak_2023 = pd.read_csv('NOAA Quality Controlled Datasets_csv/CRND20240209/2023/CRND0103-2023-AK_Aleknagik_1_NNE.csv')

# Concatenate Dataframes
df_ak_all = pd.concat([df_ak_2020, df_ak_2021, df_ak_2022, df_ak_2023])

# Remove Outliers
df_ak_all_clean = df_ak_all.drop(df_ak_all[df_ak_all['T_DAILY_MEAN'] == -9999].index)

# app.layout = html.Div([
#     html.Div(children='Daily T_MEAN - Aleknagik (AK) 2020 - 2023'),
#     dcc.Graph(figure=px.line(df_ak_all_clean, x='LST_DATE', y=df_ak_all_clean.columns[5:9]))
# ])

app.layout = html.Div(className='dash', children=[

    # Define Sidebar HTML #
    html.Div(className="sidebar", children=[
        html.Div(className="top", children=[
            html.Div(className="logo", children=[
                html.Span("NOAA USCRN"),
            ]),
            html.Img(src="assets/icons/arch.svg", id="btn", alt="image", style={'height': '50px', 'width': '50px', 'list-style-type': 'none'}),
        ]),
        html.Div(className="user"),
        html.Ul(className="nav-btn", children=[
            html.Li(children=[html.A(href='#', children=[
                html.Img(src='assets/icons/folder-1.svg', style={'height': '30px', 'width': '30px'}),
                html.Span("Folders", className="nav-item")]),
                html.Span("Folders", className="tooltip")]),

            html.Li(children=[html.A(href='#', children=[
                html.Img(src='assets/icons/clock.svg', style={'height': '30px', 'width': '30px'}),
                html.Span("Time", className="nav-item")]),
                html.Span("Time", className="tooltip")]),

            html.Li(children=[html.A(href='#', children=[html.Img(
                src='assets/icons/temperature-1.svg', style={'height': '30px', 'width': '30px'}),
                html.Span("Temperature", className="nav-item")]),
                html.Span("Temperature", className="tooltip")]),

            html.Li(children=[html.A(href='#', children=[html.Img(
                src='assets/icons/sun.svg', style={'height': '30px', 'width': '30px'}),
                html.Span("Weather", className="nav-item")]),
                html.Span("Weather", className="tooltip")]),

            html.Li(children=[html.A(href='#', children=[html.Img(
                src='assets/icons/rain-1.svg', style={'height': '30px', 'width': '30px'}),
                html.Span("Precipitation", className="nav-item")]),
                html.Span("Precipitation", className="tooltip")]),

            html.Li(children=[html.A(href='#', children=[html.Img(
                src='assets/icons/wind.svg', style={'height': '30px', 'width': '30px'}),
                html.Span("Wind", className="nav-item")]),
                html.Span("Wind", className="tooltip")]),

            html.Li(children=[html.A(href='#', children=[html.Img(
                src='assets/icons/globe.svg', style={'height': '30px', 'width': '30px'}),
                html.Span("Locations", className="nav-item")]),
                html.Span("Locations", className="tooltip")]),
        ])
    ]),

    # Define Main Content HTML #
    html.Div(className='main-content', children=[
        html.Div(className='container', children=[

            html.Div(children='Daily T_MEAN - Aleknagik (AK) 2020 - 2023'),
            dcc.Graph(figure=px.line(df_ak_all_clean, x='LST_DATE', y=df_ak_all_clean.columns[5:9]))
            # html.H1('Inner Content'),
            # html.P('https://www.youtube.com/watch?v=uy1tgKOnPB0'),
            # html.H2('Right Side'),
        ]),
    ]),
    DeferScript(src='assets/script.js'),
])

if __name__ == '__main__':
    app.run(debug=True)
