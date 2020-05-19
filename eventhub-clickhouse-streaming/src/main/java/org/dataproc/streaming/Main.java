package org.dataproc.streaming;

import com.microsoft.azure.eventhubs.EventPosition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.eventhubs.ConnectionStringBuilder;
import org.apache.spark.eventhubs.EventHubsConf;
import org.apache.spark.eventhubs.EventHubsUtils;
import org.apache.spark.sql.*;

import org.apache.spark.SparkException;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.eventhubs.EventHubsDirectDStream;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class Main {

  //   private static final String JDBC_SOURCE_PROVIDER_CLASS = DefaultSource.class.getCanonicalName();

    public static <StreamingContex> void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTest");
        String connectionString = ConnectionStringBuilder.apply(SecretsConst.EventHubConnString).build();
        EventHubsConf eventHubsConf = new EventHubsConf(connectionString);
        eventHubsConf.setConsumerGroup(SecretsConst.EventHubConsumerGroup);

        // Choose event streaming position to start from
        EventPosition.fromOffset("0");
        //EventPosition.fromSequenceNumber(100);     // Specifies sequence number 100
        //EventPosition.fromEnqueuedTime(Instant.now());// Specifies any event after the current time
        //  EventPosition.fromStartOfStream();             // Specifies from start of stream
      //  EventPosition.fromEndOfStream();               // Specifies from end of stream
        StreamingContext ssc = new StreamingContext(conf, new Duration(1000));

        SparkSession spark = SparkSession.builder().appName("DataProcStreamingTest").getOrCreate();

        EventHubsDirectDStream stream = EventHubsUtils.createDirectStream(ssc, eventHubsConf);

        stream.start();

        Dataset<Row> eventsDF =  spark.readStream().format("eventhubs")
                .options(eventHubsConf.toMap())
                .load();
        boolean streaming = eventsDF.isStreaming();// Returns True for DataFrames that have streaming sources

        eventsDF.printSchema();

        Dataset<Row> tcuEvents =  eventsDF
                .select(
                        functions.from_json(functions.col("body").cast("string"),StreamingSchemaType.eventMsgSchema).alias("Raw"),
                        //functions.unbase64(functions.col("body")).cast("string").alias("body"),
                        functions.col("properties"),
                        functions.col("enqueuedTime")
                ).select(
                        functions.from_json(functions.col("Raw.tcu_common").cast("string"),StreamingSchemaType.tcuCommon).alias("tcu_common"),
                        functions.from_json(functions.col("Raw.current_status").cast("string"),StreamingSchemaType.currentStatus).alias("current_status"),
                        functions.col("enqueuedTime")
                ).selectExpr("tcu_common.deviceid","tcu_common.message_id",  "tcu_common.datetime as event_datetime")
                .filter("current_status IS NOT NULL")
                .toDF("deviceid","message_id", "event_datetime");

        tcuEvents.printSchema();

   /*     StreamingQuery query = tcuEvents
                .writeStream()
                .outputMode("append") //.mode(SaveMode.Append)
                .format("console")
                .start();
        try {
            query.awaitTermination();
        }catch (StreamingQueryException ex){

        }
        StreamingQuery query = tcuEvents
                .writeStream()
                .format("csv")        // can be "orc", "json", "csv", etc.
                .option("checkpointLocation", "C:/Temp/checkpoint")//tmp/clickhouse-out/checkpoint
                .option("path","F:\\Maxim\\PROJECTS\\Yandex.Cloud\\DataProc\\")
                .start();
        try {
            query.awaitTermination();
        }catch (StreamingQueryException ex){

        }
*/

      StreamingQuery query2 = tcuEvents
                .writeStream()
                .outputMode("append") //            .mode(SaveMode.Append)
                .format("streaming-jdbc")
                .option(JDBCOptions.JDBC_DRIVER_CLASS(),"com.mysql.jdbc.Driver") //  "org.postgresql.Driver"
                .option(JDBCOptions.JDBC_URL(), SecretsConst.JDBC_PROVIDER_URL)
                .option(JDBCOptions.JDBC_TABLE_NAME(), SecretsConst.POSTGRE_TABLE)
                .option("user", SecretsConst.POSTGRE_USER)
                .option("password", SecretsConst.POSTGRE_PWD)
                .option("checkpointLocation", "C:/Temp/checkpoint")//tmp/clickhouse-out/checkpoint
                //.option(JDBCOptions.JDBC_BATCH_INSERT_SIZE(), "5")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();
        try {
            query2.awaitTermination();
        }catch (StreamingQueryException ex){
        }


    }

}
