package dev.irontools.flink;

import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

public class OrderSource implements SourceFunction<Order> {
    private final int totalCount;
    private final int batchSize;
    private final long delayMillis;
    private final boolean useStaticCustomerNames;
    private volatile boolean isRunning = true;

    public OrderSource(int totalCount, int batchSize, long delayMillis) {
        this(totalCount, batchSize, delayMillis, false);
    }

    public OrderSource(int totalCount, int batchSize, long delayMillis, boolean useStaticCustomerNames) {
        this.totalCount = totalCount;
        this.batchSize = batchSize;
        this.delayMillis = delayMillis;
        this.useStaticCustomerNames = useStaticCustomerNames;
    }

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        Iterable<Order> orders = OrderGenerator.generateOrdersWithDelay(
            totalCount, batchSize, delayMillis, useStaticCustomerNames);

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
