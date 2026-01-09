package dev.irontools.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

public class ProductTableSourceFactory implements DynamicTableSourceFactory {

  public static final String IDENTIFIER = "product-source";

  public static final ConfigOption<Integer> PRODUCT_COUNT =
      ConfigOptions.key("productCount")
          .intType()
          .defaultValue(10)
          .withDescription("Number of unique products to generate");

  public static final ConfigOption<Integer> UPDATE_CYCLES =
      ConfigOptions.key("updateCycles")
          .intType()
          .defaultValue(3)
          .withDescription("Number of price update cycles");

  public static final ConfigOption<Long> DELAY_MILLIS =
      ConfigOptions.key("delayMillis")
          .longType()
          .defaultValue(1000L)
          .withDescription("Delay in milliseconds between update cycles");

  public static final ConfigOption<Boolean> APPEND_ONLY =
      ConfigOptions.key("appendOnly")
          .booleanType()
          .defaultValue(false)
          .withDescription("If true, emit only INSERT events (no UPDATE_AFTER). Useful for PTFs with time semantics.");

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return new HashSet<>();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(PRODUCT_COUNT);
    options.add(UPDATE_CYCLES);
    options.add(DELAY_MILLIS);
    options.add(APPEND_ONLY);
    return options;
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    final ReadableConfig options = helper.getOptions();
    int productCount = options.get(PRODUCT_COUNT);
    int updateCycles = options.get(UPDATE_CYCLES);
    long delayMillis = options.get(DELAY_MILLIS);
    boolean appendOnly = options.get(APPEND_ONLY);

    return new ProductTableSource(
        productCount,
        updateCycles,
        delayMillis,
        appendOnly,
        context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType()
    );
  }
}
