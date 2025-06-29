import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType


# Колонка	Тип	Описание
# IATA_CODE	String	Идентификатор авиалинии
# AIRLINE	String	Название авиалинии
airlines_schema =StructType([
    StructField('IATA_CODE', StringType(), True),
    StructField('AIRLINE', StringType(), True)
])


# Колонка	Тип	Описание
# IATA_CODE	String	Идентификатор аэропорта
# AIRPORT	String	Название аэропорта
# CITY	String	Город
# STATE	String	Штат/Округ
# COUNTRY	String	Страна
# LATITUDE	Float	Широта расположения
# LONGITUDE	Float	Долгота расположения
airports_schema =StructType([
    StructField('IATA_CODE', StringType(), True),
    StructField('AIRPORT', StringType(), True),
    StructField('CITY', StringType(), True),
    StructField('STATE', StringType(), True),
    StructField('COUNTRY', StringType(), True),
    StructField('LATITUDE', DoubleType(), True),
    StructField('LONGITUDE', DoubleType(), True)
])


# Колонка	Тип	Описание
# YEAR	Integer	Год полета
# MONTH	Integer	Месяц полета
# DAY	Integer	День полета
# DAY_OF_WEEK	Integer	День недели полета [1-7] = [пн-вс]
# AIRLINE	String	Код авиалиний
# FLIGHT_NUMBER	String	Идентификатор рейса (просто ид)
# TAIL_NUMBER	String	Номер рейса
# ORIGIN_AIRPORT	String	Код аэропорта отправления
# DESTINATION_AIRPORT	String	Код аэропорта назначения
# SCHEDULED_DEPARTURE	Integer	Время запланированного отправления
# DEPARTURE_TIME	Integer	Время фактического отправления WHEEL_OFF - TAXI_OUT
# DEPARTURE_DELAY	Integer	Общая задержка отправления
# TAXI_OUT	Integer	Время, прошедшее между отправлением от выхода на посадку в аэропорту отправления и вылетом
# WHEELS_OFF	Integer	Момент времени, когда колеса самолета отрываются от земли
# SCHEDULED_TIME	Integer	Запланированное количество времени, необходимое для полета
# ELAPSED_TIME	Integer	AIR_TIME+TAXI_IN+TAXI_OUT
# AIR_TIME	Integer	Время в воздухе. Промежуток времени между WHEELS_OFF и WHEELS_ON
# DISTANCE	Integer	Расстояние между двумя аэропортами
# WHEELS_ON	Integer	Момент времени, когда колеса самолета касаются земли
# TAXI_IN	Integer	Время, прошедшее между посадкой на колеса и прибытием на посадку в аэропорту назначения
# SCHEDULED_ARRIVAL	Integer	Планируемое время прибытия
# ARRIVAL_TIME	Integer	Время когда самолет фактически прибыл в аэропорт (прибыл к гейту) WHEELS_ON+TAXI_IN
# ARRIVAL_DELAY	Integer	Время задержки в прибытии ARRIVAL_TIME-SCHEDULED_ARRIVAL
# DIVERTED	Integer	Флаг указывающий что рейс приземлился в аэропорту не по расписанию (0/1)
# CANCELLED	Integer	Флаг указывающий что рейс был отменен (0/1)
# CANCELLATION_REASON	String	Причина отмены рейса: A - Airline/Carrier; B - Weather; C - National Air System; D - Security
# AIR_SYSTEM_DELAY	Integer	Время задержки из-за воздушной системы
# SECURITY_DELAY	Integer	Время задержки из-за службы безопасности
# AIRLINE_DELAY	Integer	Время задержки по вине авиакомпании
# LATE_AIRCRAFT_DELAY	Integer	Время задержки из-за проблем самолета
# WEATHER_DELAY	Integer	Время задержки из-за погодных условий
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
    StructField('AIR_TIME', DoubleType(), True),
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


def process(spark, flights_path, airlines_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param result_path: путь с результатами преобразований
    """
    flights_fact = spark.read \
        .option("header", "true") \
        .schema(flights_schema) \
        .parquet(flights_path)

    airlines_fact = spark.read \
        .option("header", "true") \
        .schema(airlines_schema) \
        .parquet(airlines_path)

    airlines_renamed = airlines_fact.withColumnRenamed('AIRLINE', 'AIRLINE_NAME')

    datamart = flights_fact \
        .groupBy('AIRLINE') \
        .agg(
            # Рейсы без отмены и без переадресации
            f.sum(
                f.when(
                    (f.col("CANCELLED") == 0) &
                    (f.col("DIVERTED") == 0),
                    1
                ).otherwise(0)
            ).alias("correct_count"),

            # Число рейсов выполненных с задержкой (diverted)
            f.sum(
                f.when(
                    f.col("DIVERTED") == 1,
                    1
                ).otherwise(0)
            ).alias("diverted_count"),

            # Число отмененных рейсов
            f.sum(
                f.when(
                    f.col("CANCELLED") == 1,
                    1
                ).otherwise(0)
            ).alias("cancelled_count"),

            f.avg('DISTANCE').alias('avg_distance'),
            f.avg('AIR_TIME').alias('avg_air_time'),

            # Число отмен из-за проблем с самолетом
            f.sum(
                f.when(
                    f.col("CANCELLATION_REASON") == 'A',
                    1
                ).otherwise(0)
            ).alias("airline_issue_count"),

            # Число отмен из-за погодных условий
            f.sum(
                f.when(
                    f.col("CANCELLATION_REASON") == 'B',
                    1
                ).otherwise(0)
            ).alias("weather_issue_count"),

            # Число отмен из-за проблем NAS
            f.sum(
                f.when(
                    f.col("CANCELLATION_REASON") == 'C',
                    1
                ).otherwise(0)
            ).alias("nas_issue_count"),

            # Число отмен из-за службы безопасности
            f.sum(
                f.when(
                    f.col("CANCELLATION_REASON") == 'D',
                    1
                ).otherwise(0)
            ).alias("security_issue_count"),
        ) \
        .join(airlines_renamed, flights_fact['AIRLINE'] == airlines_renamed['IATA_CODE'], 'inner') \
        .select(
            'AIRLINE_NAME',
            'correct_count',
            'diverted_count',
            'cancelled_count',
            'avg_distance',
            'avg_air_time',
            'airline_issue_count',
            'weather_issue_count',
            'nas_issue_count',
            'security_issue_count',
        )

    datamart.show(truncate=True, n=10)
    datamart.write.mode('overwrite').parquet(result_path)


def main(flights_path, airlines_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob5').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    result_path = args.result_path
    main(flights_path, airlines_path, result_path)
