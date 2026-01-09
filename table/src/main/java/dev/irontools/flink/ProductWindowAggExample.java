package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ProductWindowAggExample {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.executeSql("""
      CREATE TABLE Products (
          product_id STRING,
          product_name STRING,
          price DECIMAL(10, 2),
          update_time BIGINT,
          event_time AS TO_TIMESTAMP_LTZ(update_time, 3),
          WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
      ) WITH (
          'connector' = 'product-source',
          'productCount' = '5',
          'updateCycles' = '20000',
          'delayMillis' = '5000'
      )
      """);

    // Deduplication
    // tEnv.executeSql("""
    //   SELECT
    //     product_id,
    //     product_name,
    //     price
    //   FROM (
    //     SELECT *,
    //       ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_time ASC) AS row_num
    //     FROM Products)
    //   WHERE row_num = 1
    //   """).print();

    // EXPLAIN CHANGELOG_MODE
    tEnv.executeSql("""
      SELECT
        product_name,
        window_start,
        window_end,
        ROUND(AVG(price), 2) as avg_price
      FROM TUMBLE(TABLE Products, DESCRIPTOR(event_time), INTERVAL '10' SECONDS)
      GROUP BY window_start, window_end, product_name
      """).print();
  }
}
