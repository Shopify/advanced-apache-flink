package dev.irontools.flink;

import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

public class OrderSource implements SourceFunction<Order> {
    private final int totalCount;
    private final int batchSize;
    private final long delayMillis;
    private volatile boolean isRunning = true;

    public OrderSource(int totalCount, int batchSize, long delayMillis) {
        this.totalCount = totalCount;
        this.batchSize = batchSize;
        this.delayMillis = delayMillis;
    }

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        Iterable<Order> orders = OrderGenerator.generateOrdersWithDelay(
            totalCount, batchSize, delayMillis);

        for (Order order : orders) {
            if (!isRunning) {
                break;
            }
            ctx.collect(order);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
