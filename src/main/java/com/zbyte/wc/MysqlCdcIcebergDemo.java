package com.zbyte.wc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlCdcIcebergDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("USE CATALOG default_catalog");
        tableEnv.executeSql("CREATE CATALOG s3_catalog WITH (\n" +
                "    'type' = 'iceberg',\n" +
                "    'catalog-type' = 'hadoop',\n" +
                "    'warehouse' = 'file:////Users/liwang/project/relyt-external-table/iceberg',\n" +
                "    'property-version' = '1'\n" +
                ")");
        tableEnv.executeSql("USE CATALOG s3_catalog");

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS my_database");
        tableEnv.executeSql("USE my_database");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS my_products (\n" +
                "    id INT PRIMARY KEY NOT ENFORCED,\n" +
                "    name VARCHAR\n" +
                ") WITH (\n" +
                "    'format-version'='2'\n" +
                ")");
        tableEnv.executeSql("create temporary table products (\n" +
                "    id INT,\n" +
                "    name VARCHAR,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'connection.pool.size' = '10',\n" +
                "    'hostname' = '47.98.251.75',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'dawang',\n" +
                "    'password' = 'qwerqwer',\n" +
                "    'database-name' = 'test',\n" +
                "    'table-name' = 't1'\n" +
                ")");
        // tableEnv.executeSql("SET 'execution.checkpointing.interval' = '60 s'");

        tableEnv.executeSql("INSERT INTO my_products (id,name) SELECT id, name FROM products;");
        // table.execute().print();
    }
}
