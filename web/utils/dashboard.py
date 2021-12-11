import boto3
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objs as go
import plotly.express as px
import json

BUCKET = "mha-bucket"

s3_client = boto3.client("s3")
s3 = boto3.resource("s3")


def gen_heatmap(df_dict):
    num_plots = len(df_dict)
    fig = make_subplots(rows=int(num_plots / 2) + 1, cols=2, subplot_titles=list(df_dict.keys()))
    i = 0
    row = 0
    for name, each_df in df_dict.items():
        if i % 2 == 0:
            row += 1
            col = 1
        else:
            col = 2
        i += 1
        figure = go.Heatmap(
            x=each_df.columns.tolist(),
            y=each_df.index.tolist(),
            z=each_df,
            colorbar=dict(title='Mental Health Score')
        )
        fig.add_trace(
            figure,
            row=row, col=col
        )
    fig.show()


def get_mha_data(q_number, type):
    df_dict = {}
    my_bucket = s3.Bucket(BUCKET)
    for object_summary in my_bucket.objects.filter(Prefix=f"{q_number}/"):
        file_name = object_summary.key
        if file_name.endswith('.csv'):
            obj = s3_client.get_object(Bucket=BUCKET, Key=file_name)
            if type == "income":
                my_df = pd.read_csv(obj['Body'],
                                    names=["Regions", "First Quintile", "Second Quintile", "Third Quintile",
                                           "Fourth Quintile", "Fifth Quintile"], index_col="Regions")
            else:
                my_df = pd.read_csv(obj['Body'],
                                    names=["Regions", "Less than secondary school graduation",
                                           "Secondary school graduation, no post-secondary education",
                                           "Post-secondary certificate"],
                                    index_col="Regions")
            df_dict[file_name.split('/')[1]] = my_df
    return df_dict


def get_overview_data(q_number):
    my_bucket = s3.Bucket(BUCKET)
    for object_summary in my_bucket.objects.filter(Prefix=f"{q_number}/"):
        file_name = object_summary.key
        if file_name.endswith('.csv'):
            obj = s3_client.get_object(Bucket=BUCKET, Key=file_name)
            my_df = pd.read_csv(obj['Body'],
                                names=["name", "score"])
            return my_df


def generate_geoplot(data):
    with open("data/geodata/canada_provinces.geojson", "r+") as canadaJson:
        canada_json = json.load(canadaJson)
    fig = px.choropleth(data,
                        geojson=canada_json,
                        featureidkey='properties.name',
                        locations='name',  # column in dataframe
                        color='score',  # dataframe
                        color_continuous_scale='Viridis',
                        title='Mental Health Scores in Canada (Excluding territories) for 5 years.'
                        )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.show(width=1200, height=1200)


def get_bg_data(q_number):
    df_dict = {}
    my_bucket = s3.Bucket(BUCKET)
    for object_summary in my_bucket.objects.filter(Prefix=f"{q_number}/"):
        file_name = object_summary.key
        if file_name.endswith('.csv'):
            obj = s3_client.get_object(Bucket=BUCKET, Key=file_name)
            my_df = pd.read_csv(obj['Body'], names=["GEO", "2015", "2016", "2017", "2018", "2019", "2020"])
            df_dict[file_name.split('/')[1]] = my_df
    return df_dict


def generate_bar_graphs(q_number, data):
    for file_name, each_data in data.items():
        fig = go.Figure(data=[
            go.Bar(name='2015', x=each_data['GEO'].to_numpy(), y=each_data['2015'].to_numpy()),
            go.Bar(name='2016', x=each_data['GEO'].to_numpy(), y=each_data['2016'].to_numpy()),
            go.Bar(name='2017', x=each_data['GEO'].to_numpy(), y=each_data['2017'].to_numpy()),
            go.Bar(name='2018', x=each_data['GEO'].to_numpy(), y=each_data['2018'].to_numpy()),
            go.Bar(name='2019', x=each_data['GEO'].to_numpy(), y=each_data['2019'].to_numpy()),
            go.Bar(name='2020', x=each_data['GEO'].to_numpy(), y=each_data['2020'].to_numpy())
        ])
        fig.update_layout(barmode='group')
        if q_number == "q6":
            fig.update_layout(title="Mental Health trend over the years in Canada")
        else:
            fig.update_layout(title=file_name)
        fig.show()


def handle_question(q_number):
    if q_number == "q1":
        df_dict = get_mha_data(q_number, type="income")
        return gen_heatmap(df_dict)
    if q_number == "q2":
        df_dict = get_mha_data(q_number, type="edu")
        return gen_heatmap(df_dict)
    if q_number == "q3":
        data = get_overview_data(q_number)
        generate_geoplot(data)
    if q_number == "q4":
        data = get_bg_data(q_number)
        generate_bar_graphs(q_number, data)
    if q_number == "q5":
        data = get_bg_data(q_number)
        generate_bar_graphs(q_number, data)
    if q_number == "q6":
        data = get_bg_data(q_number)
        generate_bar_graphs(q_number, data)
