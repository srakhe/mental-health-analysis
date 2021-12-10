import boto3
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objs as go

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


def get_data_from_s3(q_number):
    df_dict = {}
    my_bucket = s3.Bucket(BUCKET)
    for object_summary in my_bucket.objects.filter(Prefix=f"{q_number}/"):
        file_name = object_summary.key
        if file_name.endswith('.csv'):
            obj = s3_client.get_object(Bucket=BUCKET, Key=file_name)
            my_df = pd.read_csv(obj['Body'],
                                names=["Regions", "First Quintile", "Second Quintile", "Third Quintile",
                                       "Fourth Quintile", "Fifth Quintile"], index_col="Regions")
            df_dict[file_name.split('/')[1]] = my_df
    return df_dict


def handle_question(q_number):
    if q_number == "q1":
        df_dict = get_data_from_s3(q_number)
        return gen_heatmap(df_dict)
    if q_number == "q2":
        df_dict = get_data_from_s3(q_number)
        return gen_heatmap(df_dict)
