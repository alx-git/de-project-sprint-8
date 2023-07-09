import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType

def foreach_batch_function(df, epoch_id):
    
    df.persist()
    
    df.write.format('jdbc').\
        mode('append').\
        option('url', 'jdbc:postgresql://localhost:5432/de').\
        option('driver', 'org.postgresql.Driver').\
        option('dbtable', 'subscribers_feedback').\
        option('user', 'jovyan').\
        option('password', 'jovyan').\
        save()
    
    kafka_df = df.withColumn('value', to_json(
                          struct(col('restaurant_id'),
                          col('adv_campaign_id'),
                          col('adv_campaign_content'),
                          col('adv_campaign_owner'),
                          col('adv_campaign_owner_contact'),
                          col('adv_campaign_datetime_start'),
                          col('adv_campaign_datetime_end'),
                          col('datetime_created'),
                          col('client_id'),
                          col('trigger_datetime_created'))))
    
    kafka_df.write.format('kafka').\
        mode('append').\
        option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091').\
        option('kafka.security.protocol', 'SASL_SSL').\
        option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";').\
        option('kafka.sasl.mechanism', 'SCRAM-SHA-512').\
        option('topic', 'kovalchukalexander_out').\
        save()
    
    df.unpersist()

spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

restaurant_read_stream_df = spark.readStream. \
    format('kafka').\
    option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091').\
    option('kafka.security.protocol', 'SASL_SSL').\
    option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";').\
    option('kafka.sasl.mechanism', 'SCRAM-SHA-512').\
    option('subscribe', 'kovalchukalexander_in').\
    load()

subscribers_restaurant_df = spark.read.\
                    format('jdbc').\
                    option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de').\
                    option('driver', 'org.postgresql.Driver').\
                    option('dbtable', 'subscribers_restaurants').\
                    option('user', 'student').\
                    option('password', 'de-student').\
                    load()

incomming_message_schema = StructType([
    StructField("restaurant_id", StringType()),
    StructField("adv_campaign_id", StringType()),
    StructField("adv_campaign_content", StringType()),
    StructField("adv_campaign_owner", StringType()),
    StructField("adv_campaign_owner_contact", StringType()),
    StructField("adv_campaign_datetime_start", LongType()),
    StructField("adv_campaign_datetime_end", LongType()),
    StructField("datetime_created", LongType())
    ])

current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

filtered_read_stream_df = restaurant_read_stream_df.withColumn('value', col('value').cast(StringType())).\
    withColumn('key', col('key').cast(StringType())).\
    withColumn('event', from_json(col('value'), incomming_message_schema)).\
    selectExpr('event.*', '*').drop('event').\
    where(f'adv_campaign_datetime_start <= {current_timestamp_utc} and adv_campaign_datetime_end >= {current_timestamp_utc}')

result_df = filtered_read_stream_df.join(subscribers_restaurant_df, on="restaurant_id", how="left").\
withColumn('trigger_datetime_created', lit(current_timestamp_utc)).\
select('restaurant_id', 'adv_campaign_id', 'adv_campaign_content', 'adv_campaign_owner',
       'adv_campaign_owner_contact', 'adv_campaign_datetime_start', 'adv_campaign_datetime_end',
       'datetime_created', 'client_id', 'trigger_datetime_created').\
dropDuplicates(['adv_campaign_id', 'client_id'])

result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()