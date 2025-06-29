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
    StructField('FLIGHT_NUMBER', IntegerType(), True),
    StructField('TAIL_NUMBER', StringType(), True),
    StructField('ORIGIN_AIRPORT', StringType(), True),
    StructField('DESTINATION_AIRPORT', StringType(), True),
    StructField('SCHEDULED_DEPARTURE', IntegerType(), True),
    StructField('DEPARTURE_TIME', DoubleType(), True),
    StructField('DEPARTURE_DELAY', DoubleType(), True),
    StructField('TAXI_OUT', DoubleType(), True),
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


def process(spark, flights_path, airlines_path, airports_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param airports_path: путь до датасета c аэропортами
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

    airports_fact = spark.read \
        .option("header", "true") \
        .schema(airports_schema) \
        .parquet(airports_path)

    # # Переименовываем колонки до join, чтобы избежать конфликтов имен
    airlines_renamed = airlines_fact.withColumnRenamed('AIRLINE', 'AIRLINE_NAME')

    origin_airports = airports_fact \
        .withColumnRenamed('IATA_CODE', 'ORIGIN_IATA_CODE') \
        .withColumnRenamed('AIRPORT', 'ORIGIN_AIRPORT_NAME') \
        .withColumnRenamed('COUNTRY', 'ORIGIN_COUNTRY') \
        .withColumnRenamed('LATITUDE', 'ORIGIN_LATITUDE') \
        .withColumnRenamed('LONGITUDE', 'ORIGIN_LONGITUDE')

    dest_airports = airports_fact \
        .withColumnRenamed('IATA_CODE', 'DEST_IATA_CODE') \
        .withColumnRenamed('AIRPORT', 'DESTINATION_AIRPORT_NAME') \
        .withColumnRenamed('COUNTRY', 'DESTINATION_COUNTRY') \
        .withColumnRenamed('LATITUDE', 'DESTINATION_LATITUDE') \
        .withColumnRenamed('LONGITUDE', 'DESTINATION_LONGITUDE')

    datamart = flights_fact \
        .join(airlines_renamed, flights_fact['AIRLINE'] == airlines_renamed['IATA_CODE'], 'inner') \
        .join(origin_airports, flights_fact['ORIGIN_AIRPORT'] == origin_airports['ORIGIN_IATA_CODE'], 'inner') \
        .join(dest_airports, flights_fact['DESTINATION_AIRPORT'] == dest_airports['DEST_IATA_CODE'], 'inner') \
        .select(
            'AIRLINE_NAME',
            'TAIL_NUMBER',
            'ORIGIN_COUNTRY',
            'ORIGIN_AIRPORT_NAME',
            'ORIGIN_LATITUDE',
            'ORIGIN_LONGITUDE',
            'DESTINATION_COUNTRY',
            'DESTINATION_AIRPORT_NAME',
            'DESTINATION_LATITUDE',
            'DESTINATION_LONGITUDE'
        )

    # Count and print the number of completed flights
    completed_flights_count = datamart.count()
    print(f"Количество выполненных рейсов: {completed_flights_count}")

    # print(f"Размерность результата: ({datamart.count()}, {len(datamart.columns)})")
    datamart.show(truncate=True, n=10)
    datamart.write.mode('overwrite').parquet(result_path)


def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, airports_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--airports_path', type=str, default='airports.parquet', help='Please set airports datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)
