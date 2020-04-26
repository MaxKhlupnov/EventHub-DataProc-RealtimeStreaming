package org.dataproc.streaming;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class StreamingSchemaType {

    public static StructType CsvSchema = new StructType()
            .add("PartitionKey",  DataTypes.StringType,  false)
            .add("RowKey",DataTypes.StringType,false)
            .add("Timestamp", DataTypes.TimestampType,false)
            .add("accel_pedal_position", DataTypes.IntegerType,true)
            .add("alarm_event", DataTypes.IntegerType,true)
            .add("altitude", DataTypes.DoubleType,true)
            .add("ambient_air_temperature", DataTypes.DoubleType,true)
            .add("battery_soc", DataTypes.IntegerType,true)
            .add("battery_voltage", DataTypes.DoubleType,true);

}
