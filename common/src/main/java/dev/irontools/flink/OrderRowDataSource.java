package dev.irontools.flink;

import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;

public class OrderRowDataSource implements SourceFunction<RowData> {
    private final int totalCount;
    private final int batchSize;
    private final long delayMillis;
    private volatile boolean isRunning = true;

    public OrderRowDataSource(int totalCount, int batchSize, long delayMillis) {
        this.totalCount = totalCount;
        this.batchSize = batchSize;
        this.delayMillis = delayMillis;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        Iterable<Order> orders = OrderGenerator.generateOrdersWithDelay(
            totalCount, batchSize, delayMillis);

        for (Order order : orders) {
            if (!isRunning) {
                break;
            }
            ctx.collect(convertToRowData(order));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private RowData convertToRowData(Order order) {
        GenericRowData rowData = new GenericRowData(6);
        rowData.setField(0, StringData.fromString(order.getOrderId()));
        rowData.setField(1, StringData.fromString(order.getCustomerName()));
        rowData.setField(2, StringData.fromString(order.getCategory()));
        rowData.setField(3, DecimalData.fromBigDecimal(BigDecimal.valueOf(order.getAmount()), 10, 2));
        rowData.setField(4, StringData.fromString(order.getProductName()));
        rowData.setField(5, order.getTimestamp());
        rowData.setRowKind(RowKind.INSERT);
        return rowData;
    }
}
