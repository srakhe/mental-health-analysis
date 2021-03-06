from pyspark.sql import SparkSession, functions, types
from pyspark.ml import Pipeline
from pyspark.sql.types import FloatType
from pyspark.ml.feature import VectorAssembler, PCA
from pyspark.ml.feature import MinMaxScaler
import sys

assert sys.version_info >= (3, 5)


def data_etl(input_dataframe, selected_characteristic):
    data_selected = input_dataframe.select('REF_DATE', 'GEO', 'selected_characteristic', 'Indicators',
                                           'Characteristics', 'VALUE', 'STATUS')
    data_selected = data_selected.filter(data_selected.selected_characteristic.startswith(selected_characteristic))
    data_selected_selected = data_selected.filter(data_selected.Characteristics == 'Percent')
    data_selected_selected = data_selected_selected.filter(
        (data_selected_selected.STATUS == 'E') | (functions.isnull(data_selected_selected.STATUS)))
    data_selected_filtered = data_selected_selected.filter(
        (data_selected_selected.Indicators == 'Perceived mental health, very good or excellent') | (
                data_selected_selected.Indicators == 'Perceived mental health, fair or poor') | (
                data_selected_selected.Indicators == 'Perceived life stress, most days quite a bit or extremely stressful') | (
                data_selected_selected.Indicators == 'Mood disorder') | (
                data_selected_selected.Indicators == 'Sense of belonging to local community, somewhat strong or very strong') | (
                data_selected_selected.Indicators == 'Life satisfaction, satisfied or very satisfied'))

    data_selected_filtered_pivoted = data_selected_filtered.groupBy("REF_DATE", "GEO", "selected_characteristic").pivot(
        "Indicators").avg("VALUE").orderBy('REF_DATE')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed(
        'Perceived mental health, very good or excellent', 'good_mental_health')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed(
        'Perceived mental health, fair or poor', 'poor_mental_health')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed(
        'Perceived life stress, most days quite a bit or extremely stressful', 'extremely_stressful')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Mood disorder', 'mood_disorder')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed(
        'Sense of belonging to local community, somewhat strong or very strong', 'high_belonging')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed(
        'Life satisfaction, satisfied or very satisfied', 'high_life_satisftn')

    return data_selected_filtered_pivoted


def handle_na(input_dataframe):
    all_values = input_dataframe.agg(
        {'good_mental_health': 'avg', 'poor_mental_health': 'avg', 'extremely_stressful': 'avg', 'mood_disorder': 'avg',
         'high_belonging': 'avg', 'high_life_satisftn': 'avg'}).collect()[0]

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


def attempt_pca(input_dataframe, year):
    input_dataframe = input_dataframe.filter(input_dataframe['REF_DATE'] == year)
    data_assembler = VectorAssembler(
        inputCols=['good_mental_health', 'poor_mental_health', 'extremely_stressful', 'mood_disorder', 'high_belonging',
                   'high_life_satisftn'], outputCol="features")
    pca = PCA(k=1, inputCol="features", outputCol="pca_features")
    preprocessing_pipeline = Pipeline(stages=[data_assembler, pca])

    pca_model = preprocessing_pipeline.fit(input_dataframe)
    result = pca_model.transform(input_dataframe)
    firstelementabs = functions.udf(lambda v: abs(float(v[0])), FloatType())
    result = result.withColumn('pca_features_mod', firstelementabs("pca_features")).select('GEO',
                                                                                           'selected_characteristic',
                                                                                           'pca_features_mod')

    data_assembler2 = VectorAssembler(inputCols=['pca_features_mod'], outputCol="features")
    result_vector = data_assembler2.transform(result)

    # result = result.filter(result['REF_DATE'] == year).select('GEO', 'selected_characteristic', 'pca_features')
    scaler = MinMaxScaler(inputCol="features", outputCol="mh_score_output", max=100.0, min=1.0)
    scalerModel = scaler.fit(result_vector)
    scaledResults = scalerModel.transform(result_vector)

    firstelement = functions.udf(lambda v: float(v[0]), FloatType())
    resultFinal = scaledResults.withColumn("mh_score", firstelement("mh_score_output")).select('GEO',
                                                                                               'selected_characteristic',
                                                                                               'mh_score')

    return resultFinal


def heatmap_formating(input_dataframe, characterstic_to_study):
    resultsForHeatmap = input_dataframe.groupBy("GEO").pivot("selected_characteristic").avg('mh_score')
    if characterstic_to_study == 'Household income':
        resultsForHeatmap = resultsForHeatmap.select('GEO', 'Household income, first quintile',
                                                     'Household income, second quintile',
                                                     'Household income, third quintile',
                                                     'Household income, fourth quintile',
                                                     'Household income, fifth quintile')
    elif characterstic_to_study == 'Highest level of education':
        resultsForHeatmap = resultsForHeatmap.select('GEO',
                                                     'Highest level of education, less than secondary school graduation',
                                                     'Highest level of education, post-secondary certificate/diploma or university degree',
                                                     'Highest level of education, secondary school graduation, no post-secondary education')
    else:
        print("Wrong characterstic_to_study")
    return resultsForHeatmap


def main(inputs, output, characterstic_to_study):
    pages_schema = types.StructType([
        types.StructField('REF_DATE', types.IntegerType()),
        # Specifies years in which data was collected. Unique values(6): 2015, 2016, 2017, 2018, 2019, 2020
        types.StructField('GEO', types.StringType()),
        # Specifies the Canadian Province in which the survey respondent resides. 11 Unique values in total.
        types.StructField('DGUID', types.StringType()),  # Unique Identifier
        types.StructField('Selected characteristic', types.StringType()),
        # Income or education category the respondent belongs to (5 income level category, 3 education level category)
        types.StructField('Indicators', types.StringType()),  # Self Reporting of health status of respondant
        types.StructField('Characteristics', types.StringType()),  # Measure of value(Percentage, number, etc)
        types.StructField('UOM', types.StringType()),  # Number (6 values)
        types.StructField('UOM_ID', types.IntegerType()),  # 223 (3 values)
        types.StructField('SCALAR_FACTOR', types.StringType()),  # units (categorical, 1 value)
        types.StructField('SCALAR_ID', types.IntegerType()),  # 0
        types.StructField('VECTOR', types.StringType()),  # v111666340
        types.StructField('COORDINATE', types.StringType()),  # 1.1.1.1
        types.StructField('VALUE', types.DoubleType()),  # 3013900
        types.StructField('STATUS', types.StringType()),  # E/F
        types.StructField('SYMBOL', types.StringType()),  # NO VALUES
        types.StructField('TERMINATED', types.StringType()),  # NO VALUES
        types.StructField('DECIMALS', types.IntegerType())  # 0/1
    ])

    data_loaded = spark.read.csv(inputs, schema=pages_schema, sep=',', header=True).withColumnRenamed(
        'Selected characteristic',
        'selected_characteristic')

    data_selected_filtered_pivoted = data_etl(data_loaded, characterstic_to_study).cache()
    all_unique_years = data_selected_filtered_pivoted.select('REF_DATE').distinct().collect()
    data_selected_filtered_pivoted_filled = handle_na(data_selected_filtered_pivoted).cache()

    for year_val in all_unique_years:
        year_val = year_val[0]
        resultFinal = attempt_pca(data_selected_filtered_pivoted_filled, str(year_val))
        resultsForHeatmap = heatmap_formating(resultFinal, characterstic_to_study)
        resultsForHeatmap.repartition(1).write.mode("overwrite").csv(output + str(year_val))


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    characterstic_to_study = sys.argv[3]
    if characterstic_to_study == "0":
        characterstic_to_study = "Household income"
    elif characterstic_to_study == "1":
        characterstic_to_study = "Highest level of education"
    spark = SparkSession.builder.appName('Mental Health PCA').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output, characterstic_to_study)
