package dev.irontools.flink;

import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;

public class ProductRowDataSource implements SourceFunction<RowData> {
  private final int productCount;
  private final int updateCycles;
  private final long delayMillis;
  private volatile boolean isRunning = true;

  public ProductRowDataSource(int productCount, int updateCycles, long delayMillis) {
    this.productCount = productCount;
    this.updateCycles = updateCycles;
    this.delayMillis = delayMillis;
  }

  @Override
  public void run(SourceContext<RowData> ctx) throws Exception {
    Iterable<ProductGenerator.ProductWithMetadata> products =
        ProductGenerator.generateProductsWithUpdates(productCount, updateCycles, delayMillis);

    for (ProductGenerator.ProductWithMetadata productWithMetadata : products) {
      if (!isRunning) {
        break;
      }
      ctx.collect(convertToRowData(productWithMetadata));
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  private RowData convertToRowData(ProductGenerator.ProductWithMetadata productWithMetadata) {
    Product product = productWithMetadata.getProduct();
    GenericRowData rowData = new GenericRowData(4);
    rowData.setField(0, StringData.fromString(product.getProductId()));
    rowData.setField(1, StringData.fromString(product.getProductName()));
    rowData.setField(2, DecimalData.fromBigDecimal(BigDecimal.valueOf(product.getPrice()), 10, 2));
    rowData.setField(3, product.getUpdateTime());
    if (productWithMetadata.isUpdate()) {
      rowData.setRowKind(RowKind.UPDATE_AFTER);
    } else {
      rowData.setRowKind(RowKind.INSERT);
    }
    return rowData;
  }
}
