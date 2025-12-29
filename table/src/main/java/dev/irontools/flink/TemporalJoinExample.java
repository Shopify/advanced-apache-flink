package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class TemporalJoinExample {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.executeSql("""
      CREATE TABLE Orders (
        order_id STRING,
        customer_name STRING,
        category STRING,
        amount DECIMAL(10, 2),
        product_id STRING,
        product_name STRING,
        `timestamp` BIGINT,
        order_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
        WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
      ) WITH (
        'connector' = 'order-source',
        'totalCount' = '500',
        'delayMillis' = '2000'
      )
      """);

    tEnv.executeSql("""
      CREATE TABLE Products (
        product_id STRING,
        product_name STRING,
        price DECIMAL(10, 2),
        update_time BIGINT,
        update_time_ts AS TO_TIMESTAMP_LTZ(update_time, 3),
        WATERMARK FOR update_time_ts AS update_time_ts - INTERVAL '5' SECOND,
        PRIMARY KEY (product_id) NOT ENFORCED
      ) WITH (
        'connector' = 'product-source',
        'productCount' = '5',
        'updateCycles' = '3',
        'delayMillis' = '5000'
      )
      """);

    tEnv.executeSql("""
      SELECT
        o.order_id,
        o.customer_name,
        o.product_id,
        o.product_name,
        o.order_time,
        p.price AS product_price_at_order_time
      FROM Orders AS o
      LEFT JOIN Products FOR SYSTEM_TIME AS OF o.order_time AS p
        ON o.product_id = p.product_id
      """).print();
  }
}
