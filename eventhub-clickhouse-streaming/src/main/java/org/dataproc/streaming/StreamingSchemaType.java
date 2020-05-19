package org.dataproc.streaming;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class StreamingSchemaType {

    public static StructType eventMsgSchema = new StructType()
            .add("edge_message_id",  DataTypes.StringType,   true)
            .add("message_type", DataTypes.IntegerType,true)
            .add("EventEnqueuedUtcTime", DataTypes.TimestampType,true)
            .add("EventProcessedUtcTime",DataTypes.TimestampType, true)
            .add("tcu_common", DataTypes.StringType,true)
            .add("current_status", DataTypes.StringType,true);

    public  static StructType tcuCommon = new StructType()
            .add("datetime",DataTypes.StringType, false) //DataTypes.TimestampType
            .add("deviceid",DataTypes.StringType, false)
            .add("vin",DataTypes.StringType, true)
            .add("esn",DataTypes.StringType, true)
            .add("iccid",DataTypes.StringType, true)
            .add("message_id",DataTypes.IntegerType, true)
            .add("request_messageid",DataTypes.IntegerType, true);

    public  static StructType currentStatus = new StructType()
            .add("datetime",DataTypes.TimestampType, false)
            .add("gpsinfo",DataTypes.StringType, false)
            .add("window_position",DataTypes.StringType, true)
            .add("wheels_tpms_status",DataTypes.StringType, true)
            .add("safety_belts_status",DataTypes.StringType, true)
            .add("vehicle_alarm_event",DataTypes.StringType, true);

/*
    gpsinfo": {
            "datetime": "2019-07-31T14:19:57Z",
            "latitude": -12.957319999999999,
            "longitude": -38.757235999999999,
            "course": 111,
            "heading": 0,
            "altitude": 0.000000000000000,
            "speed": 182,
            "satqty": 0
}*/

}
