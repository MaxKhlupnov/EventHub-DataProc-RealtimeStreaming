package org.dataproc.streaming;

public final class SecretsConst {

     /* A validAzure Event or IoT Hub (Event endpoint) connection string which will be used to connect to
     *                      an EventHubs instance. A connection string can be obtained from the Azure portal
     * */
     final static String EventHubConnString = "Endpoint=sb://iothub-ns-prod-squad-2293180-d0434c40e5.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=E5dkit27LC8wtyN0Ira+1F7STBJ2YJs6eikC0wimi/E=;EntityPath=prod-squadron-iothub";

     final static String EventHubConsumerGroup = "$Default";

     public static final String POSTGRE_USER = "makhlu";
     public static final String POSTGRE_PWD = "P@ssw0rd.1";
     public static final String JDBC_PROVIDER_URL ="jdbc:mysql://rc1c-uqbxklabdpwt8ol1.mdb.yandexcloud.net:3306/CurrentStatus?useSSL=true";
             //"jdbc:postgresql://rc1c-tq7wombzdmr96892.mdb.yandexcloud.net:6432/CurrentStatus?&targetServerType=master&ssl=true&sslmode=require";
     public static final String POSTGRE_TABLE = "AzureStreaming";
}
