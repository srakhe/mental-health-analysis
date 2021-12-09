from pyspark import SparkConf, SparkContext
import json
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import avg
from pyspark.ml import Pipeline
from pyspark.sql.types import FloatType
from pyspark.ml.feature import VectorAssembler, PCA
from pyspark.ml.feature import MinMaxScaler
import sys
assert sys.version_info >= (3, 5)
#import plotly.express as px

def main(inputs, outputs):
    pages_schema = types.StructType([
        types.StructField('REF_DATE', types.IntegerType()),#2015
        types.StructField('GEO', types.StringType()),#Canada (excluding territories)
        types.StructField('DGUID', types.StringType()),#2016A000210
        types.StructField('Selected characteristic', types.StringType()),#Household income, first quintile
        types.StructField('Indicators', types.StringType()),#Perceived health, very good or excellent
        types.StructField('Characteristics', types.StringType()),#Number of persons
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

    data_loaded = spark.read.csv(inputs, schema=pages_schema, sep = ',', header = True).withColumnRenamed('Selected characteristic', 'selected_characteristic').withColumnRenamed('Indicators', 'IndicatorsIndicatorsIndicatorsIndicators')
    # print("Table 1")
    # data_loaded.show(100, truncate = False)
    data_selected = data_loaded.select('REF_DATE', 'GEO', 'selected_characteristic', 'IndicatorsIndicatorsIndicatorsIndicators', 'Characteristics', 'VALUE', 'STATUS')
    data_selected = data_selected.filter(~data_selected.selected_characteristic.startswith('Highest level of education'))#.filter(data_selected.Characteristics == 'Percent').filter(data_selected.STATUS != 'F')
    # print("Table 2")
    # data_selected.show(100, truncate = False)
    data_selected_selected = data_selected.filter(data_selected.Characteristics == 'Percent')#.filter(data_selected.STATUS != 'F')
    # print("Table 3")
    # data_selected_selected.show(100, truncate = False)



    data_selected_selected = data_selected_selected.filter((data_selected_selected.STATUS == 'E') | (functions.isnull(data_selected_selected.STATUS)))
    # print("Table 4")
    # data_selected_selected.show(100, truncate = False)

    #data_selected_filtered = data_selected_selected.filter(data_selected_selected['IndicatorsIndicatorsIndicatorsIndicators'] == 'Perceived mental health, very good or excellent')
    data_selected_filtered = data_selected_selected.filter((data_selected_selected.IndicatorsIndicatorsIndicatorsIndicators == 'Perceived mental health, very good or excellent') | (data_selected_selected.IndicatorsIndicatorsIndicatorsIndicators == 'Perceived mental health, fair or poor') | (data_selected_selected.IndicatorsIndicatorsIndicatorsIndicators == 'Perceived life stress, most days quite a bit or extremely stressful') | (data_selected_selected.IndicatorsIndicatorsIndicatorsIndicators == 'Mood disorder') | (data_selected_selected.IndicatorsIndicatorsIndicatorsIndicators == 'Sense of belonging to local community, somewhat strong or very strong') | (data_selected_selected.IndicatorsIndicatorsIndicatorsIndicators == 'Life satisfaction, satisfied or very satisfied'))

    # print("Table 5")
    # data_selected_filtered.orderBy('REF_DATE').show(100, truncate = False)

    #
    # print("Table 6")
    data_selected_filtered_pivoted = data_selected_filtered.groupBy("REF_DATE", "GEO", "selected_characteristic").pivot("IndicatorsIndicatorsIndicatorsIndicators").avg("VALUE").orderBy('REF_DATE')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('selected_characteristic', 'selected_characteristicSelectedCharacteristic')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Perceived mental health, very good or excellent', 'good_mental_health')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Perceived mental health, fair or poor', 'poor_mental_health')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Perceived life stress, most days quite a bit or extremely stressful', 'extremely_stressful')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Mood disorder', 'mood_disorder')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Sense of belonging to local community, somewhat strong or very strong', 'high_belonging')
    data_selected_filtered_pivoted = data_selected_filtered_pivoted.withColumnRenamed('Life satisfaction, satisfied or very satisfied', 'high_life_satisftn').cache()

# optimization like "df.na.drop(Seq("c_name")).select(avg(col("c_name")))" needed for the section below

    good_mental_health = data_selected_filtered_pivoted.select(avg("good_mental_health")).collect()[0][0]
    poor_mental_health = data_selected_filtered_pivoted.select(avg("poor_mental_health")).collect()[0][0]
    extremely_stressful = data_selected_filtered_pivoted.select(avg("extremely_stressful")).collect()[0][0]
    mood_disorder = data_selected_filtered_pivoted.select(avg("mood_disorder")).collect()[0][0]
    high_belonging = data_selected_filtered_pivoted.select(avg("high_belonging")).collect()[0][0]
    high_life_satisftn = data_selected_filtered_pivoted.select(avg("high_life_satisftn")).collect()[0][0]

    data_selected_filtered_pivoted_filled = data_selected_filtered_pivoted.fillna(good_mental_health, subset=["good_mental_health"])
    data_selected_filtered_pivoted_filled = data_selected_filtered_pivoted_filled.fillna(poor_mental_health, subset=["poor_mental_health"])
    data_selected_filtered_pivoted_filled = data_selected_filtered_pivoted_filled.fillna(extremely_stressful, subset=["extremely_stressful"])
    data_selected_filtered_pivoted_filled = data_selected_filtered_pivoted_filled.fillna(mood_disorder, subset=["mood_disorder"])
    data_selected_filtered_pivoted_filled = data_selected_filtered_pivoted_filled.fillna(high_belonging, subset=["high_belonging"])
    data_selected_filtered_pivoted_filled = data_selected_filtered_pivoted_filled.fillna(high_life_satisftn, subset=["high_life_satisftn"])

    #data_selected_filtered_pivoted_filled.show(600, truncate = False)

    data_assembler = VectorAssembler(inputCols=['good_mental_health', 'poor_mental_health', 'extremely_stressful', 'mood_disorder', 'high_belonging', 'high_life_satisftn'], outputCol="features")
    pca = PCA(k=1, inputCol="features", outputCol="pca_features")
    preprocessing_pipeline = Pipeline(stages=[data_assembler, pca])
    pca_model = preprocessing_pipeline.fit(data_selected_filtered_pivoted_filled)


    result =  pca_model.transform(data_selected_filtered_pivoted_filled)#.select('pca_features')
    #firstelement=functions.udf(lambda v:float(v[0]),FloatType())
    #result = result.withColumn("mh_score_unscaled", firstelement("pca_features"))
    result = result.filter(result['REF_DATE'] == '2020').select('GEO', 'selected_characteristicSelectedCharacteristic', 'pca_features')#.withColumn("mh_score", result["pca_features"][1])#.withColumn("mh_score", result.pca_features)
    scaler = MinMaxScaler(inputCol="pca_features", outputCol="mh_score_output", max=100.0, min=1.0)
    scalerModel = scaler.fit(result)
    scaledResults = scalerModel.transform(result)

    firstelement=functions.udf(lambda v:float(v[0]),FloatType())
    resultFinal = scaledResults.withColumn("mh_score", firstelement("mh_score_output")).select('GEO', 'selected_characteristicSelectedCharacteristic', 'mh_score')
    resultFinal.show(600, truncate=False)
    resultsForHeatmap = resultFinal.groupBy("GEO").pivot("selected_characteristicSelectedCharacteristic").avg('mh_score')


    resultsForHeatmap.show(600, truncate=False)
    resultsForHeatmap.write.csv(outputs)
    resultsForHeatmapPandas = resultsForHeatmap.toPandas()
    fig = px.imshow(resultsForHeatmapPandas)
    fig.show()

    #pca_model.transform(data_selected_filtered_pivoted).collect()[0].output
    #pca_column = pca_model.transform(data_selected_filtered_pivoted_filled).collect()#[0]
    # print("PCA Column:")
    # print(pca_column)


if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('Mental Health PCA').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, outputs)
