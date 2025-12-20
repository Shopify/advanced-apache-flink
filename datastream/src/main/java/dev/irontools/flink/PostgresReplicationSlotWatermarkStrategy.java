package dev.irontools.flink;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * WatermarkStrategy that calculates watermarks based on PostgreSQL replication slot lag.
 *
 * <p>This strategy demonstrates how to integrate replication lag monitoring into Flink's
 * watermarking mechanism. The watermark delay dynamically adjusts based on how far behind
 * the replication slot is from the current WAL position.
 *
 * <p>Usage example:
 * <pre>{@code
 * WatermarkStrategy<Order> strategy =
 *     PostgresReplicationSlotWatermarkStrategy
 *         .<Order>create()
 *         .withTimestampAssigner((order, timestamp) -> order.getTimestamp());
 *
 * stream.assignTimestampsAndWatermarks(strategy);
 * }</pre>
 *
 * @param <T> The type of elements in the stream
 */
public class PostgresReplicationSlotWatermarkStrategy<T> implements WatermarkStrategy<T> {

    private final TimestampAssigner<T> timestampAssigner;

    /**
     * Private constructor. Use {@link #create()} to instantiate.
     *
     * @param timestampAssigner Optional timestamp assigner for extracting timestamps from events
     */
    private PostgresReplicationSlotWatermarkStrategy(TimestampAssigner<T> timestampAssigner) {
        this.timestampAssigner = timestampAssigner;
    }

    /**
     * Creates a new PostgresReplicationSlotWatermarkStrategy.
     *
     * <p>Remember to call {@link #withTimestampAssigner(TimestampAssigner)} to specify
     * how to extract timestamps from your events.
     *
     * @param <T> The type of elements in the stream
     * @return A new watermark strategy instance
     */
    public static <T> PostgresReplicationSlotWatermarkStrategy<T> create() {
        return new PostgresReplicationSlotWatermarkStrategy<>(null);
    }

    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new PostgresReplicationSlotWatermarkGenerator<>();
    }

    @Override
    public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        // Return the user-provided timestamp assigner, or a no-op if none was set
        if (timestampAssigner != null) {
            return timestampAssigner;
        } else {
            // No-op assigner: returns the current timestamp (from Flink's internal tracking)
            return (element, recordTimestamp) -> recordTimestamp;
        }
    }

    public WatermarkStrategy<T> withTimestampAssigner(TimestampAssigner<T> timestampAssigner) {
        return new PostgresReplicationSlotWatermarkStrategy<>(timestampAssigner);
    }
}
