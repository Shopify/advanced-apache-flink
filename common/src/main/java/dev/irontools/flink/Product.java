package dev.irontools.flink;

import java.util.Objects;

public class Product {
  private String productId;
  private String productName;
  private Double price;
  private Long updateTime;

  public Product() {
  }

  public Product(String productId, String productName, Double price, Long updateTime) {
    this.productId = productId;
    this.productName = productName;
    this.price = price;
    this.updateTime = updateTime;
  }

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public String getProductName() {
    return productName;
  }

  public void setProductName(String productName) {
    this.productName = productName;
  }

  public Double getPrice() {
    return price;
  }

  public void setPrice(Double price) {
    this.price = price;
  }

  public Long getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(Long updateTime) {
    this.updateTime = updateTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Product product = (Product) o;
    return Objects.equals(productId, product.productId) &&
            Objects.equals(productName, product.productName) &&
            Objects.equals(price, product.price) &&
            Objects.equals(updateTime, product.updateTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(productId, productName, price, updateTime);
  }

  @Override
  public String toString() {
    return "Product{" +
            "productId='" + productId + '\'' +
            ", productName='" + productName + '\'' +
            ", price=" + price +
            ", updateTime=" + updateTime +
            '}';
  }
}
