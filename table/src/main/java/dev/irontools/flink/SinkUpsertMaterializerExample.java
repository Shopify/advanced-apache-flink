package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class SinkUpsertMaterializerExample {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        settings.getConfiguration().setString("execution.checkpointing.interval", "10s");
        // settings.getConfiguration().setString("table.exec.sink.upsert-materialize", "NONE");
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("""
            CREATE TABLE Orders (
                order_id STRING,
                customer_name STRING,
                category STRING,
                amount DECIMAL(10, 2),
                product_id STRING,
                product_name STRING,
                `timestamp` BIGINT,
                event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'order-source',
                'totalCount' = '10000',
                'delayMillis' = '5000'
            )
            """);

        tableEnv.executeSql("""
            CREATE TABLE Products (
                product_id STRING,
                product_name STRING,
                price DECIMAL(10, 2),
                update_time BIGINT,
                event_time AS TO_TIMESTAMP_LTZ(update_time, 3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
                PRIMARY KEY (product_id) NOT ENFORCED
            ) WITH (
                'connector' = 'product-source',
                'productCount' = '5',
                'updateCycles' = '5000',
                'delayMillis' = '3000'
            )
            """);

        tableEnv.executeSql("""
            CREATE TABLE CustomSink (
                category STRING,
                customerName STRING,
                price DECIMAL(10, 2),
                PRIMARY KEY (category) NOT ENFORCED
            ) WITH (
                'connector' = 'print'
            )
            """);

        tableEnv.executeSql("""
            CREATE TABLE StatsSink (
                category STRING,
                customer_name STRING,
                total_orders BIGINT,
                total_revenue DECIMAL(10, 2),
                avg_price DECIMAL(10, 2),
                PRIMARY KEY (customer_name) NOT ENFORCED
            ) WITH (
                'connector' = 'print'
            )
            """);

        tableEnv.executeSql("""
            INSERT INTO StatsSink
            SELECT
                o.category,
                LAST_VALUE(o.customer_name) as customer_name,
                COUNT(*) as total_orders,
                SUM(o.amount) as total_revenue,
                AVG(p.price) as avg_price
            FROM Orders o
            INNER JOIN Products p ON o.product_id = p.product_id
            GROUP BY o.category
            """).print();

        // EXPLAIN CHANGELOG_MODE
        // tableEnv.executeSql("""
        //     INSERT INTO CustomSink
        //     SELECT category, customer_name, price
        //     FROM Orders o
        //     INNER JOIN Products p ON o.product_id = p.product_id
        //     """).print();
    }
}
