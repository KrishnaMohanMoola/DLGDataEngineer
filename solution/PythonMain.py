import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# This method takes the parquet file as the input and returns the maximum temperature
def get_max_temp(df):

    window = Window.orderBy(col("Screen_Temperature").desc())
    return df.withColumn("max_temp",rank().over(window)).filter(col("max_temp")==1).drop(col("max_temp"))

# This method takes the csv files and retrieves only the required fields i.e. Region, Observation Date and Screen Temperature
# for the group of region and observation date the first step takes the maximum screen temperature
# this details will be saved in a parquet file format at the output location
def load_parquet(spark: SparkSession, output_file_path, args):

    try:
        df = spark.read.csv(args, header=True).select(col("Region"),
                                                  col("ObservationDate").cast("Date").alias("Observation_Date"),
                                                  col("ScreenTemperature").cast("Decimal(5,2)")) \
        .groupBy(col("region"), col("Observation_Date")) \
        .agg(max(col("ScreenTemperature")).alias("Screen_Temperature")) \
        .select(col("region"), col("Observation_Date"), col("Screen_Temperature"))

        df.coalesce(1).write.mode('overwrite').parquet(output_file_path)

    except Exception as e:
        print('Error while reading csv file or while writing into Parquet - Error Details - {c}, Message, {m}'.format(c = type(e).__name__, m = str(e)))
        exit(0)

if __name__ == "__main__":
    spark_session = (
            SparkSession.builder
                        .master("local[2]")
                        .appName("DataTest")
                        .config("spark.executorEnv.PYTHONHASHSEED", "0")
                        .getOrCreate()
    )

    input_file_path = '/tmp/DLG/DataEngineering/Weather/Input/'
    output_file_path = '/tmp/DLG/DataEngineering/Weather/Output/'

    # Get the file names from the input folder and copy the file names into a list
    directories = os.listdir(input_file_path)
    file_names = [input_file_path + x for x in directories if 'csv' in x]

    # check the length of the list (file_names), if there are files then process the file else there are no files to process
    if (len(file_names) > 0):
        load_parquet(spark_session, output_file_path, file_names)

        df_read_file = spark_session.read.parquet(output_file_path)
        max_temp_date_region = get_max_temp(df_read_file)
        max_temp_date_region.show()

    else:
        print(f'File Does not exists in path - {input_file_path}')


