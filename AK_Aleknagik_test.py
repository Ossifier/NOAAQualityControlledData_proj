from dash import Dash, html, dash_table, dcc
import pandas as pd
import plotly.express as px

app = Dash(__name__)

# Import Binned Data:
df_ak_2020 = pd.read_csv('NOAA Quality Controlled Datasets_csv/CRND20240208/2020/CRND0103-2020-AK_Aleknagik_1_NNE.csv')
df_ak_2021 = pd.read_csv('NOAA Quality Controlled Datasets_csv/CRND20240208/2021/CRND0103-2021-AK_Aleknagik_1_NNE.csv')
df_ak_2022 = pd.read_csv('NOAA Quality Controlled Datasets_csv/CRND20240208/2022/CRND0103-2022-AK_Aleknagik_1_NNE.csv')
df_ak_2023 = pd.read_csv('NOAA Quality Controlled Datasets_csv/CRND20240208/2023/CRND0103-2023-AK_Aleknagik_1_NNE.csv')

# Concatenate Dataframes
df_ak_all = pd.concat([df_ak_2020, df_ak_2021, df_ak_2022, df_ak_2023])

# Remove Outliers
df_ak_all_clean = df_ak_all.drop(df_ak_all[df_ak_all['T_DAILY_MEAN'] == -9999].index)


app.layout = html.Div([
    html.Div(children='Daily T_MEAN - Aleknagik (AK) 2020 - 2023'),
    dcc.Graph(figure=px.line(df_ak_all_clean, x='LST_DATE', y=df_ak_all_clean.columns[5:9]))
])

if __name__ == '__main__':
    app.run(debug=True)
