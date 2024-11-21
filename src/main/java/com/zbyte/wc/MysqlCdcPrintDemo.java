package com.zbyte.wc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MysqlCdcPrintDemo {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname("47.98.251.75")
                        .port(3306)
                        .databaseList("test")
                        .tableList("test.t1")
                        .username("dawang")
                        .password("qwerqwer")
                        .serverId("6401-6404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .serverTimeZone("UTC+8")
                        .includeSchemaChanges(true) // output the schema changes as well
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
                .setParallelism(1)
                .print()
                .setParallelism(1);

        env.execute();
    }
}
