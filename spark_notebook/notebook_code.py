import pyspark.sql.functions as sql_funcs
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json


#CELL  1 - reading data from kafka and pushing it to elasticsearch:
conn_string = (
    "<EVENT_HUB_CONNECTION_STRING>"
)

endTime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

starting_event_position = {
    "offset": "-1",
    "seqNo": -1,  # not in use
    "enqueuedTime": None,  # not in use
    "isInclusive": True,
}

ending_event_position = {
    "offset": None,  # not in use
    "seqNo": -1,  # not in use
    "enqueuedTime": endTime,
    "isInclusive": True,
}

conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        conn_string
    ),
    "eventhubs.startingPosition": json.dumps(starting_event_position),
    "eventhubs.endingPosition": json.dumps(ending_event_position),
}

json_fields = [
    "incident_key",
    "occur_date",
    "occur_time",
    "boro",
    "loc_of_occur_desc",
    "precinct",
    "jurisdiction_code",
    "loc_classfctn_desc",
    "location_desc",
    "statistical_murder_flag",
    "perp_age_group",
    "perp_sex",
    "perp_race",
    "vic_age_group",
    "vic_sex",
    "vic_race",
    "x_coord_cd",
    "y_coord_cd",
    "latitude",
    "longitude",
    "geocoded_column",
    ":@computed_region_yeji_bk3q",
    ":@computed_region_92fq_4b7q",
    ":@computed_region_sbqj_enih",
    ":@computed_region_efsh_h5xi",
    ":@computed_region_f5dn_yrer",
]

read_df = spark.readStream.format("eventhubs").options(**conf).load()

read_schema = StructType([StructField(field, StringType())
                          for field in json_fields])

decoded_df = (
    read_df.selectExpr("cast(body as string) as json_data")
        .select(from_json("json_data", read_schema).alias("table"))
        .select("table.*")
)

for active_stream in spark.streams.active:
    active_stream.stop()

stream_to_es_query = decoded_df.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/temp/") \
    .option("es.resource", "<ES_INDEX_NAME>") \
    .option("es.nodes", "<ES_ENDPOINT>") \
    .option("es.port", "443") \
    .option("es.net.ssl", "true") \
    .option("es.nodes.wan.only", "true") \
    .option("es.net.http.auth.user", "<ES_USER>") \
    .option("es.net.http.auth.pass", "<ES_PASSWORD>") \
    .option("es.index.auto.create", "true") \
    .start()

stream_to_es_query.awaitTermination(100)
#ENF OF CELL 1



#CELL 2 - queries which aggregate dataframes
month_count_df = decoded_df \
    .groupBy(
        window("occur_date", "30 days")) \
    .count()

month_count_df = month_count_df.withColumn("start", sql_funcs.col("window").getItem("start")) \
    .withColumn("end", sql_funcs.col("window").getItem("end")).orderBy('start')

month_count_df = month_count_df.drop("window")

race_stats_df = (
    decoded_df.groupBy("perp_race").count().orderBy('perp_race')
)

day_count_df = (
    decoded_df.groupBy("occur_date").count().orderBy('occur_date')
)

def write_stream_to_table(current_stream, query_name):
    current_stream.writeStream \
        .format("memory") \
        .queryName(query_name) \
        .trigger(processingTime="5 seconds") \
        .outputMode("complete") \
        .start() \
        .awaitTermination(20)


write_stream_to_table(month_count_df, 'q1')
write_stream_to_table(race_stats_df, 'q2')
write_stream_to_table(day_count_df, 'q3')
#ENF OF CELL 2



#CELL 3 - view aggregated data using sql
%%sql
select * from q1;
select * from q2;
select * from q3
#ENF OF CELL 3
