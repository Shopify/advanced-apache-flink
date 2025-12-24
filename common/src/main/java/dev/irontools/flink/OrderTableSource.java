package dev.irontools.flink;

import org.apache.flink.legacy.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.DataType;

public class OrderTableSource implements ScanTableSource {

    private final int totalCount;
    private final int batchSize;
    private final long delayMillis;
    private final DataType dataType;

    public OrderTableSource(int totalCount, int batchSize, long delayMillis, DataType dataType) {
        this.totalCount = totalCount;
        this.batchSize = batchSize;
        this.delayMillis = delayMillis;
        this.dataType = dataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        OrderRowDataSource orderRowDataSource = new OrderRowDataSource(totalCount, batchSize, delayMillis);
        return SourceFunctionProvider.of(orderRowDataSource, true);
    }

    @Override
    public DynamicTableSource copy() {
        return new OrderTableSource(totalCount, batchSize, delayMillis, dataType);
    }

    @Override
    public String asSummaryString() {
        return "OrderTableSource";
    }
}
