from pyspark import SparkConf, SparkContext
import json
from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5)

def main(inputs):
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
    print("Table 1")
    data_loaded.show(100, truncate = False)
    data_selected = data_loaded.select('REF_DATE', 'GEO', 'selected_characteristic', 'IndicatorsIndicatorsIndicatorsIndicators', 'Characteristics', 'VALUE', 'STATUS')
    data_selected = data_selected.filter(~data_selected.selected_characteristic.startswith('Highest level of education'))#.filter(data_selected.Characteristics == 'Percent').filter(data_selected.STATUS != 'F')
    print("Table 2")
    data_selected.show(100, truncate = False)
    data_selected_selected = data_selected.filter(data_selected.Characteristics == 'Percent')#.filter(data_selected.STATUS != 'F')
    print("Table 3")
    data_selected_selected.show(100, truncate = False)



    data_selected_selected = data_selected_selected.filter((data_selected_selected.STATUS == 'E') | (functions.isnull(data_selected_selected.STATUS)))
    print("Table 4")
    data_selected_selected.show(100, truncate = False)

    #data_selected_filtered = data_selected_selected.filter(data_selected_selected['IndicatorsIndicatorsIndicatorsIndicators'] == 'Perceived mental health, very good or excellent')
    data_selected_filtered = data_selected_selected.filter((data_selected_selected.IndicatorsIndicatorsIndicatorsIndicators == 'Perceived mental health, very good or excellent') | (data_selected_selected.IndicatorsIndicatorsIndicatorsIndicators == 'Perceived mental health, fair or poor') | (data_selected_selected.IndicatorsIndicatorsIndicatorsIndicators == 'Perceived life stress, most days quite a bit or extremely stressful') | (data_selected_selected.IndicatorsIndicatorsIndicatorsIndicators == 'Mood disorder') | (data_selected_selected.IndicatorsIndicatorsIndicatorsIndicators == 'Sense of belonging to local community, somewhat strong or very strong') | (data_selected_selected.IndicatorsIndicatorsIndicatorsIndicators == 'Life satisfaction, satisfied or very satisfied'))

    print("Table 5")
    data_selected_filtered.orderBy('REF_DATE').show(100, truncate = False)


    print("Table 6")
    data_selected_filtered_pivoted = data_selected_filtered.groupBy("REF_DATE", "GEO", "selected_characteristic").pivot("IndicatorsIndicatorsIndicatorsIndicators").avg("VALUE").orderBy('REF_DATE')
    data_selected_filtered_pivoted.withColumnRenamed('selected_characteristic', 'selected_characteristicselected_characteristicselected_characteristic')
    data_selected_filtered_pivoted.show(600, truncate = False)

if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('Mental Health PCA').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)
