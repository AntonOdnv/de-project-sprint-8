from time import sleep
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# Настроим логгер
logging.basicConfig(level=logging.ERROR)  # или logging.DEBUG для более подробного логирования
logger = logging.getLogger(__name__)

# Задаем имена входящего и исходящего топиков
TOPIC_NAME_IN = 'student.topic.cohort20.antodnv'
TOPIC_NAME_OUT = 'student.topic.cohort20.antodnv.out'

# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
current_timestamp_utc = int(round(unix_timestamp(current_timestamp())))

# Необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# Задаем опции подключения к kafka
kafka_security_options = {
    'kafka.bootstrap.servers':'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

# Задаем опции подключения к БД Postgre с данными о подписках на рестораны
postgresql_restaurants_settings = {
    "url": "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de",
    "dbtable": "public.subscribers_restaurants",
    "driver": "org.postgresql.Driver",
    'user': 'jovyan',
    'password': 'jovyan'
}

# Задаем опции подключения к локальной БД Postgre, куда будем сохранять результат с полем feedback
postgresql_feedback_settings = {
    "url": "jdbc:postgresql://localhost:5432/de",
    "dbtable": "public.subscribers_feedback",
    "driver": "org.postgresql.Driver",
    'user': 'jovyan',
    'password': 'jovyan'
}

# Определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType()),
    StructField("adv_campaign_id", StringType()),
    StructField("adv_campaign_content", StringType()),
    StructField("adv_campaign_owner", StringType()),
    StructField("adv_campaign_owner_contact", StringType()),
    StructField("adv_campaign_datetime_start", LongType()),
    StructField("adv_campaign_datetime_end", LongType()),
    StructField("datetime_created", LongType()),
])


# Создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
def spark_init(spark_name = 'RestaurantSubscribeStreamingService') -> SparkSession:

    spark = (
        SparkSession.builder.appName(spark_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", spark_jars_packages)
        .getOrCreate()
    )

    return spark


# Читаем из топика Kafka сообщения с акциями от ресторанов.
def read_kafka_stream(spark: SparkSession, options: dict) -> DataFrame:
    df = (spark.readStream
          .format('kafka')
          .options(**options)
          .option("subscribe", TOPIC_NAME_IN)
          .load())
    
    return df

# Обрабатываем полученные данные: десериализуем, обогащаем и фильтруем
def filter_stream_data(df: DataFrame, schema: StructType, current_timestamp_utc:TimestampType) -> DataFrame:
    df = (df
          .withColumn('value', f.col('value').cast(StringType()))
          .withColumn('event', f.from_json(f.col('value'), schema))
          .selectExpr('event.*')
          .withColumn('timestamp',
                      f.from_unixtime(f.col('timestamp'), "yyyy-MM-dd' 'HH:mm:ss.SSS")
                      .cast(TimestampType()))
          .dropDuplicates(['client_id', 'timestamp'])
          .withWatermark('timestamp', '5 minutes')
          .withColumn('trigger_datetime_created', current_timestamp_utc)
          .filter('''trigger_datetime_created >= adv_campaign_datetime_start 
                     and 
                     adv_campaign_datetime_end >= trigger_datetime_created''')
          )
    
    return df


# Получаем из БД информацию о пользователях и их подписках на рестораны
def subscribers_restaurant(spark: SparkSession, options: dict) -> DataFrame:
    df = (spark.read
            .format("jdbc")
            .options(**options)
            .load()
            .dropDuplicates(['client_id', 'restaurant_id'])
            .select('client_id', 'restaurant_id')
            )  
    
    return df


# Готовим объединенный датафрейм с акциями ресторанов для пользователей с подписками
def join(df_stream: DataFrame, df_subscribers: DataFrame) -> DataFrame:
    return df_stream.join(df_subscribers, 'restaurant_id', how='inner')


# Записываем данные в 2 канала: локальную БД Postgre и в топик kafka
def foreach_batch_function(df: DataFrame):
    # Сохраняем df в память, чтобы не читать повторно для повторной записи (Spark lazy evaluation)
    df.persist()

    # Задаем нужные нам колонки
    columns = [
    'restaurant_id',
    'adv_campaign_id',
    'adv_campaign_content',
    'adv_campaign_owner',
    'adv_campaign_owner_contact',
    'adv_campaign_datetime_start',
    'adv_campaign_datetime_end',
    'datetime_created',
    'trigger_datetime_created',
    'client_id'
            ]
    
    # Записываем df в Postgre в таблицу subscribers_feedback. Добавляем поле feedback
    try:
        ( df.select(columns) 
            .withColumn('feedback', f.lit('')) 
            .write.format("jdbc") 
            .mode('append') 
            .options(**postgresql_feedback_settings) 
            .save()
        )
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}")
    
    # Пишем в топик kafka
    try:
        ( df.select(f.to_json(f.struct(columns)).alias('value')) 
            .write 
            .mode("append") 
            .format("kafka") 
            .options(**kafka_security_options) 
            .option("topic", TOPIC_NAME_OUT) 
            .save()
        )
    except Exception as e:
        logger.error(f"Error writing to Kafka: {str(e)}")

    # Удаляем сохраненный df из памяти
    df.unpersist()


# Собираем и запускаем код
if __name__ == "__main__":
    spark = spark_init('RestaurantSubscribeStreamingService')
    restaurant_read_stream_df = read_kafka_stream(spark, kafka_security_options)
    filtered_read_stream_df = filter_stream_data(restaurant_read_stream_df, 
                                                   incomming_message_schema, current_timestamp_utc)
    subscribers_restaurant_df = subscribers_restaurant(spark, postgresql_restaurants_settings)
    output = join(filtered_read_stream_df, subscribers_restaurant_df)
    query = (output
             .writeStream
             .outputMode("append")
             .format("kafka")
             .options(**kafka_security_options)
             .option("topic", TOPIC_NAME_OUT)
             .trigger(processingTime="15 seconds")
             .foreachBatch(foreach_batch_function)
             .option("truncate", False)
             .start())

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        sleep(30)

    query.awaitTermination()
