from pyspark import SparkConf, SparkContext
import json
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import avg
from pyspark.ml import Pipeline
from pyspark.sql.types import FloatType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import MinMaxScaler
import sys
assert sys.version_info >= (3, 5)

def data_etl(input_dataframe, selected_characteristic):
     data_selected = input_dataframe.select('REF_DATE', 'GEO', 'selected_characteristic', 'Indicators', 'Characteristics', 'VALUE', 'STATUS')
     data_selected = data_selected.filter(data_selected.selected_characteristic.startswith(selected_characteristic))
     data_selected_selected = data_selected.filter(data_selected.Characteristics == 'Percent')
     data_selected_selected = data_selected_selected.filter((data_selected_selected.STATUS == 'E') | (functions.isnull(data_selected_selected.STATUS)))
     data_selected_filtered = data_selected_selected.filter((data_selected_selected.Indicators == 'Perceived mental health, very good or excellent') | (data_selected_selected.Indicators == 'Perceived mental health, fair or poor') | (data_selected_selected.Indicators == 'Perceived life stress, most days quite a bit or extremely stressful') | (data_selected_selected.Indicators == 'Mood disorder') | (data_selected_selected.Indicators == 'Sense of belonging to local community, somewhat strong or very strong') | (data_selected_selected.Indicators == 'Life satisfaction, satisfied or very satisfied'))

     data_selected_filtered_pivoted = data_selected_filtered.groupBy("GEO").pivot("Indicators").avg("VALUE")
     data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Perceived mental health, very good or excellent', 'good_mental_health')
     data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Perceived mental health, fair or poor', 'poor_mental_health')
     data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Perceived life stress, most days quite a bit or extremely stressful', 'extremely_stressful')
     data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Mood disorder', 'mood_disorder')
     data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Sense of belonging to local community, somewhat strong or very strong', 'high_belonging')
     data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Life satisfaction, satisfied or very satisfied', 'high_life_satisftn')

     return data_selected_filtered_pivoted


def handle_na(input_dataframe):
    all_values = input_dataframe.agg({'good_mental_health': 'avg', 'poor_mental_health': 'avg', 'extremely_stressful': 'avg', 'mood_disorder': 'avg', 'high_belonging': 'avg', 'high_life_satisftn': 'avg'}).collect()[0]

    good_mental_health = all_values['avg(good_mental_health)']
    poor_mental_health = all_values['avg(poor_mental_health)']
    extremely_stressful = all_values['avg(extremely_stressful)']
    mood_disorder = all_values['avg(mood_disorder)']
    high_belonging = all_values['avg(high_belonging)']
    high_life_satisftn = all_values['avg(high_life_satisftn)']

    input_dataframe = input_dataframe.fillna(good_mental_health, subset=["good_mental_health"])
    input_dataframe = input_dataframe.fillna(poor_mental_health, subset=["poor_mental_health"])
    input_dataframe = input_dataframe.fillna(extremely_stressful, subset=["extremely_stressful"])
    input_dataframe = input_dataframe.fillna(mood_disorder, subset=["mood_disorder"])
    input_dataframe = input_dataframe.fillna(high_belonging, subset=["high_belonging"])
    input_dataframe = input_dataframe.fillna(high_life_satisftn, subset=["high_life_satisftn"])

    return input_dataframe

def get_mh_score(input_dataframe):

    data_assembler = VectorAssembler(inputCols=['good_mental_health', 'poor_mental_health', 'extremely_stressful', 'mood_disorder', 'high_belonging', 'high_life_satisftn'], outputCol="features")
    df_vector = data_assembler.transform(input_dataframe).select("features")
    matrix = Correlation.corr(df_vector, "features").collect()[0]["pearson({})".format("features")].toArray()

    results = input_dataframe.withColumn('mh_score', input_dataframe.good_mental_health*matrix[5][0] + input_dataframe.poor_mental_health*matrix[5][1] + input_dataframe.extremely_stressful*matrix[5][2] + input_dataframe.mood_disorder*matrix[5][3] + input_dataframe.high_belonging*matrix[5][4])# + input_dataframe.high_life_satisftn)
    results = results.select('GEO', 'mh_score')

    data_assembler2 = VectorAssembler(inputCols=['mh_score'], outputCol="features")
    df_vector2 = data_assembler2.transform(results)
    scaler = MinMaxScaler(inputCol="features", outputCol="mh_score_output", max=100.0, min=1.0)
    scalerModel = scaler.fit(df_vector2)
    scaledResults = scalerModel.transform(df_vector2)

    firstelement = functions.udf(lambda v:float(v[0]),FloatType())
    resultFinal = scaledResults.withColumn("mh_score", firstelement("mh_score_output")).select('GEO', 'mh_score')
    return resultFinal

def main(inputs, characterstic_to_study):
    pages_schema = types.StructType([
        types.StructField('REF_DATE', types.IntegerType()),#Specifies years in which data was collected. Unique values(6): 2015, 2016, 2017, 2018, 2019, 2020
        types.StructField('GEO', types.StringType()),#Specifies the Canadian Provience in which the survey respondent resides. 11 Unique values in total.
        types.StructField('DGUID', types.StringType()),#Unique Identifier
        types.StructField('Selected characteristic', types.StringType()),#Income or education category the respondant belongs to (5 income level category, 3 education level category)
        types.StructField('Indicators', types.StringType()),#Self Reporting of health status of respondant
        types.StructField('Characteristics', types.StringType()),#Measure of value(Percentage, number, etc)
        types.StructField('UOM', types.StringType()),#Number (6 values)
        types.StructField('UOM_ID', types.IntegerType()),#223 (3 values)
        types.StructField('SCALAR_FACTOR', types.StringType()),#units (categorical, 1 value)
        types.StructField('SCALAR_ID', types.IntegerType()),#0
        types.StructField('VECTOR', types.StringType()),#v111666340
        types.StructField('COORDINATE', types.StringType()),#1.1.1.1
        types.StructField('VALUE', types.DoubleType()),#3013900
        types.StructField('STATUS', types.StringType()),#E/F
        types.StructField('SYMBOL', types.StringType()),#NO VALUES
        types.StructField('TERMINATED', types.StringType()),#NO VALUES
        types.StructField('DECIMALS', types.IntegerType())#0/1
    ])

    data_loaded = spark.read.csv(inputs, schema=pages_schema, sep = ',', header = True).withColumnRenamed('Selected characteristic', 'selected_characteristic')#.withColumnRenamed('Indicators', 'IndicatorsIndicatorsIndicatorsIndicators')

    data_selected_filtered_pivoted = data_etl(data_loaded, characterstic_to_study).cache()
    data_selected_filtered_pivoted_filled = handle_na(data_selected_filtered_pivoted).cache()
    resultFinal = get_mh_score(data_selected_filtered_pivoted_filled)
    resultFinal.show(600, truncate=False)
    resultFinal.repartition(1).write.csv(str(year_val))




if __name__ == '__main__':
    inputs = sys.argv[1]
    characterstic_to_study = sys.argv[2]
    spark = SparkSession.builder.appName('Mental Health PCA').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, characterstic_to_study)
