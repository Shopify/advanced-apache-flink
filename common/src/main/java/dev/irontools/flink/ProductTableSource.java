package dev.irontools.flink;

import org.apache.flink.legacy.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.DataType;

public class ProductTableSource implements ScanTableSource {

  private final int productCount;
  private final int updateCycles;
  private final long delayMillis;
  private final boolean appendOnly;
  private final DataType dataType;

  public ProductTableSource(int productCount, int updateCycles, long delayMillis, boolean appendOnly, DataType dataType) {
    this.productCount = productCount;
    this.updateCycles = updateCycles;
    this.delayMillis = delayMillis;
    this.appendOnly = appendOnly;
    this.dataType = dataType;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return appendOnly ? ChangelogMode.insertOnly() : ChangelogMode.all();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
    ProductRowDataSource productRowDataSource = new ProductRowDataSource(productCount, updateCycles, delayMillis, appendOnly);
    return SourceFunctionProvider.of(productRowDataSource, true);
  }

  @Override
  public DynamicTableSource copy() {
    return new ProductTableSource(productCount, updateCycles, delayMillis, appendOnly, dataType);
  }

  @Override
  public String asSummaryString() {
    return "ProductTableSource";
  }
}
