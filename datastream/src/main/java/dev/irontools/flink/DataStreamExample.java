package dev.irontools.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class DataStreamExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Order> orders = OrderGenerator.generateOrders(20);

        env.fromData(orders)
            .filter(order -> order.getAmount() > 100.0)
            // .map(new MapFunction<Order, Tuple2<String, Double>>() {
            //     @Override
            //     public Tuple2<String, Double> map(Order order) throws Exception {
            //         return Tuple2.of(order.getCategory(), order.getAmount());
            //     }
            // })
            // .keyBy(t -> t.f0)
            // .sum("f1")
            .map(order -> {
                return String.format("Order: %s | %s | $%.2f | %s",
                    order.getOrderId(),
                    order.getCategory(),
                    order.getAmount(),
                    order.getCustomerName());
            })
            .print();

        env.execute("DataStream Example with Orders");
    }
}
