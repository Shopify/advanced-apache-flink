package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ProductPatternMatchExample {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    // 'appendOnly' = 'true'
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
          'delayMillis' = '1000'
      )
      """);

    tEnv.executeSql("""
      SELECT *
      FROM Products
      MATCH_RECOGNIZE (
        PARTITION BY product_id
        ORDER BY event_time
        MEASURES
          BASELINE.product_name AS product_name,
          BASELINE.price AS baseline_price,
          LAST(INCREASE.price) AS peak_price,
          ROUND((LAST(INCREASE.price) - BASELINE.price) / BASELINE.price * 100, 2) AS percent_increase,
          BASELINE.event_time AS baseline_time,
          LAST(INCREASE.event_time) AS peak_time,
          COUNT(INCREASE.price) AS consecutive_increases
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (BASELINE INCREASE+?)
        DEFINE
          INCREASE AS INCREASE.price > BASELINE.price
      ) AS price_changes
      """).print();
  }
}
