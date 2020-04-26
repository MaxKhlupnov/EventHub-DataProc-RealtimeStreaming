package org.dataproc.streaming;

import com.microsoft.azure.eventhubs.EventPosition;
import org.apache.spark.SparkConf;
import org.apache.spark.eventhubs.ConnectionStringBuilder;
import org.apache.spark.eventhubs.EventHubsConf;
import org.apache.spark.eventhubs.EventHubsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.eventhubs.EventHubsDirectDStream;
import org.apache.spark.api.java.function.FlatMapFunction;

public class Main {

    public static <StreamingContex> void Main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTest");
        String connectionString = ConnectionStringBuilder.apply(SecretsConst.EventHubConnString).build();
        EventHubsConf eventHubsConf = new EventHubsConf(connectionString);
        eventHubsConf.setConsumerGroup(SecretsConst.EventHubConsumerGroup);

        // Choose event streaming position to start from
        //EventPosition.fromOffset("0");
        //EventPosition.fromSequenceNumber(100);     // Specifies sequence number 100
        //EventPosition.fromEnqueuedTime(Instant.now());// Specifies any event after the current time
        //  EventPosition.fromStartOfStream();             // Specifies from start of stream
        EventPosition.fromEndOfStream();               // Specifies from end of stream
        StreamingContext ssc = new StreamingContext(conf, new Duration(1000));

        SparkSession spark = SparkSession.builder().appName("DataProcStreamingTest").getOrCreate();

        EventHubsDirectDStream stream = EventHubsUtils.createDirectStream(ssc, eventHubsConf);

        stream.start();

        Dataset<Row> eventsDF =  spark.readStream().format("eventhubs")
                .options(eventHubsConf.toMap())
                .load();
        boolean streaming = eventsDF.isStreaming();// Returns True for DataFrames that have streaming sources

        eventsDF.printSchema();
        StreamingSchemaType userSchema = new StreamingSchemaType();

        eventsDF.select(functions.col("body").cast("string"),
                //functions.from_json(functions.col("body").cast("string"),userSchema).alias("Raw"),
                functions.col("properties"),
                functions.col("enqueuedTime")
        ).selectExpr("Raw.*", "properties", "enqueuedTime")
                .withWatermark("enqueuedTime", "60 seconds")
                .createOrReplaceTempView("Messages");

        StreamingQuery query = eventsDF.sqlContext().sql("SELECT * FROM Messages")
                .writeStream().outputMode("append")
                .format("console").start();

        try {
            query.awaitTermination();
        }catch (StreamingQueryException ex){

        }

    }

}
