package dev.irontools.flink;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DataStreamWindowAggExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Iterable<Order> orders = OrderGenerator.generateOrdersWithDelay(5000000, 5, 5000, false);

        env.fromCollection(orders.iterator(), Order.class)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
            )
            .filter(order -> order.getAmount() > 100.0)
            .map(new MapFunction<Order, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> map(Order order) throws Exception {
                    return Tuple2.of(order.getCategory(), order.getAmount());
                }
            })
            .keyBy(t -> t.f0)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
            .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1),
                new WindowFunction<Tuple2<String, Double>, String, String, TimeWindow>() {
                  @Override
                  public void apply(
                      String key,
                      TimeWindow window,
                      Iterable<Tuple2<String, Double>> input,
                      Collector<String> out
                  ) {
                    Tuple2<String, Double> agg = input.iterator().next();
                    out.collect(String.format(
                        "Window [%d - %d] Key: %s, Sum: %.2f",
                        window.getStart(), window.getEnd(), key, agg.f1
                    ));
                  }
                }
            )
            .print();

        env.execute("DataStream Example with Orders");
    }
}
