import boto3
import pandas as pd

BUCKET = "public-cmpt-732"

s3_client = boto3.client("s3")
s3 = boto3.resource("s3")


def handle_question(q_number, q_value):
    if q_number == "q1":
        file_path = "data/q1/your_file.csv"
        my_bucket = s3.Bucket(BUCKET)
        for object_summary in my_bucket.objects.filter(Prefix="customers/"):
            file_name = object_summary.key
            # file_name = file_name.lstrip("customers/")
            obj = s3_client.get_object(Bucket=BUCKET, Key=file_name)
            my_df = pd.read_csv(obj['Body'])
            print(my_df.head())


handle_question("q1", "q1")
