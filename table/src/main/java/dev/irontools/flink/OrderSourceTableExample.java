package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class OrderSourceTableExample {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.executeSql("""
      CREATE TABLE orders (
        order_id STRING,
        customer_name STRING,
        category STRING,
        amount DECIMAL(10, 2),
        product_name STRING,
        `timestamp` BIGINT
      ) WITH (
        'connector' = 'order-source',
        'totalCount' = '100'
      )
      """);

    tEnv.executeSql("""
      SELECT
        category,
        ROUND(SUM(amount), 2) as total_amount
      FROM orders
      WHERE amount > 100.0
      GROUP BY category
      """).print();
  }
}
