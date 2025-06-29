import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType


flights_schema = StructType([
    StructField('YEAR', IntegerType(), True),
    StructField('MONTH', IntegerType(), True),
    StructField('DAY', IntegerType(), True),
    StructField('DAY_OF_WEEK', IntegerType(), True),
    StructField('AIRLINE', StringType(), True),
    StructField('FLIGHT_NUMBER', StringType(), True),
    StructField('TAIL_NUMBER', StringType(), True),
    StructField('ORIGIN_AIRPORT', StringType(), True),
    StructField('DESTINATION_AIRPORT', StringType(), True),
    StructField('SCHEDULED_DEPARTURE', IntegerType(), True),
    StructField('DEPARTURE_TIME', IntegerType(), True),
    StructField('DEPARTURE_DELAY', IntegerType(), True),
    StructField('TAXI_OUT', IntegerType(), True),
    StructField('WHEELS_OFF', IntegerType(), True),
    StructField('SCHEDULED_TIME', IntegerType(), True),
    StructField('ELAPSED_TIME', IntegerType(), True),
    StructField('AIR_TIME', IntegerType(), True),
    StructField('DISTANCE', IntegerType(), True),
    StructField('WHEELS_ON', IntegerType(), True),
    StructField('TAXI_IN', IntegerType(), True),
    StructField('SCHEDULED_ARRIVAL', IntegerType(), True),
    StructField('ARRIVAL_TIME', IntegerType(), True),
    StructField('ARRIVAL_DELAY', IntegerType(), True),
    StructField('DIVERTED', IntegerType(), True),
    StructField('CANCELLED', IntegerType(), True),
    StructField('CANCELLATION_REASON', StringType(), True),
    StructField('AIR_SYSTEM_DELAY', IntegerType(), True),
    StructField('SECURITY_DELAY', IntegerType(), True),
    StructField('AIRLINE_DELAY', IntegerType(), True),
    StructField('LATE_AIRCRAFT_DELAY', IntegerType(), True),
    StructField('WEATHER_DELAY', IntegerType(), True)
])


def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """
    flights_fact = spark.read \
        .option("header", "true") \
        .schema(flights_schema) \
        .parquet(flights_path)

    datamart = flights_fact \
        .where(flights_fact['TAIL_NUMBER'].isNotNull()) \
        .groupBy(flights_fact['TAIL_NUMBER']) \
        .agg(f.count('*').alias('count')) \
        .select(f.col('TAIL_NUMBER'), f.col('count')) \
        .orderBy(f.col('count').desc(), f.col('TAIL_NUMBER')) \
        .limit(10)

    datamart.show(truncate=False, n=1000)
    datamart.write.mode('overwrite').parquet(result_path)


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob1').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
