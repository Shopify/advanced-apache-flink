package dev.irontools.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

public class OrderTableSourceFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "order-source";

    public static final ConfigOption<Integer> TOTAL_COUNT =
        ConfigOptions.key("totalCount")
            .intType()
            .defaultValue(1000)
            .withDescription("Total number of orders to generate");

    public static final ConfigOption<Integer> BATCH_SIZE =
        ConfigOptions.key("batchSize")
            .intType()
            .defaultValue(10)
            .withDescription("Number of orders per batch");

    public static final ConfigOption<Long> DELAY_MILLIS =
        ConfigOptions.key("delayMillis")
            .longType()
            .defaultValue(100L)
            .withDescription("Delay in milliseconds between batches");

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
        options.add(TOTAL_COUNT);
        options.add(BATCH_SIZE);
        options.add(DELAY_MILLIS);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig options = helper.getOptions();
        int totalCount = options.get(TOTAL_COUNT);
        int batchSize = options.get(BATCH_SIZE);
        long delayMillis = options.get(DELAY_MILLIS);

        return new OrderTableSource(
            totalCount,
            batchSize,
            delayMillis,
            context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType()
        );
    }
}
