package dev.irontools.flink;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

public class ProductGenerator {
  private static final Random RANDOM = new Random();
  private static final String[] PRODUCT_NAMES = {
    "Laptop",
    "Mouse",
    "Keyboard",
    "Monitor",
    "Headphones"
  };

  public static Iterable<ProductWithMetadata> generateProductsWithUpdates(
      int productCount, int updateCycles, long delayMillis) {
    return () -> new ProductUpdateIterator(productCount, updateCycles, delayMillis);
  }

  private static Product generateProduct(String productId, String productName, Long updateTime) {
    double price = 10.0 + (RANDOM.nextDouble() * 990.0);
    return new Product(productId, productName, price, updateTime);
  }

  public static class ProductWithMetadata {
    private final Product product;
    private final boolean isUpdate;

    public ProductWithMetadata(Product product, boolean isUpdate) {
      this.product = product;
      this.isUpdate = isUpdate;
    }

    public Product getProduct() {
      return product;
    }

    public boolean isUpdate() {
      return isUpdate;
    }
  }

  public static class ProductUpdateIterator implements Iterator<ProductWithMetadata>, Serializable {
    private final int productCount;
    private final int updateCycles;
    private final long delayMillis;
    private int currentCycle = 0;
    private int currentProductIndex = 0;
    private long currentUpdateTime;
    private final String[] productIds;
    private final String[] productNames;

    public ProductUpdateIterator(int productCount, int updateCycles, long delayMillis) {
      this.productCount = Math.min(productCount, PRODUCT_NAMES.length);
      this.updateCycles = updateCycles;
      this.delayMillis = delayMillis;
      this.currentUpdateTime = System.currentTimeMillis();
      this.productIds = new String[this.productCount];
      this.productNames = new String[this.productCount];

      for (int i = 0; i < this.productCount; i++) {
        this.productIds[i] = "P" + (i + 1);
        this.productNames[i] = PRODUCT_NAMES[i];
      }
    }

    @Override
    public boolean hasNext() {
      return currentCycle <= updateCycles;
    }

    @Override
    public ProductWithMetadata next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException();
      }

      if (currentProductIndex == 0 && currentCycle > 0) {
        try {
          Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while pausing", e);
        }
        currentUpdateTime = System.currentTimeMillis();
      }

      String productId = productIds[currentProductIndex];
      String productName = productNames[currentProductIndex];
      Product product = generateProduct(productId, productName, currentUpdateTime);
      boolean isUpdate = currentCycle > 0;

      currentProductIndex++;
      if (currentProductIndex >= productCount) {
        currentProductIndex = 0;
        currentCycle++;
      }

      return new ProductWithMetadata(product, isUpdate);
    }
  }
}
