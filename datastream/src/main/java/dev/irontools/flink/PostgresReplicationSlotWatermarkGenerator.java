package dev.irontools.flink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * WatermarkGenerator that demonstrates calculating watermarks based on PostgreSQL replication slot lag.
 *
 * <p>This implementation uses a LSN-to-timestamp mapping approach:
 * <ol>
 *   <li>Periodically queries current LSN and replication slot LSN (stubbed)</li>
 *   <li>Records each LSN observation with its timestamp</li>
 *   <li>Looks up when the replication slot's current LSN was observed</li>
 *   <li>Calculates watermark lag based on the time difference</li>
 * </ol>
 *
 * <p>This is a demonstration implementation with stub methods - it doesn't actually connect to PostgreSQL.
 *
 * @param <T> The type of elements in the stream
 */
public class PostgresReplicationSlotWatermarkGenerator<T> implements WatermarkGenerator<T>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresReplicationSlotWatermarkGenerator.class);

    private static final long DEFAULT_WATERMARK_DELAY_MS = 3000L;
    private static final int MAP_CLEANUP_THRESHOLD = 100;

    // Maximum observed event timestamp
    private long currentMaxTimestamp = Long.MIN_VALUE;

    // Map tracking when each LSN value was observed
    private final Map<String, Long> lsnToTimestampMap = new HashMap<>();

    // Current LSN position from latest query
    private String currentLsn;

    // LSN position at the replication slot (lags behind current)
    private String replicationSlotLsn;

    // Counter for periodic map cleanup
    private int periodicEmitCount = 0;

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        // Track the maximum observed event timestamp
        // We don't query Postgres per event (too expensive) - queries happen in onPeriodicEmit
        if (eventTimestamp > currentMaxTimestamp) {
            currentMaxTimestamp = eventTimestamp;
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // Only emit watermarks if we've seen at least one event
        if (currentMaxTimestamp == Long.MIN_VALUE) {
            LOG.debug("No events observed yet, skipping watermark emission");
            return;
        }

        // Query current LSN from Postgres (stub)
        currentLsn = getCurrentLsn();

        // Record this LSN observation with current time
        long currentTime = System.currentTimeMillis();
        lsnToTimestampMap.put(currentLsn, currentTime);

        LOG.debug("Recorded LSN observation: {} at time {}", currentLsn, currentTime);

        // Query replication slot LSN (stub)
        replicationSlotLsn = getReplicationSlotLsn();

        // Calculate watermark based on when we observed the slot's LSN
        long watermark = calculateWatermark();

        // Emit watermark if valid
        if (watermark > Long.MIN_VALUE) {
            output.emitWatermark(new Watermark(watermark));

            long lag = currentTime - lsnToTimestampMap.getOrDefault(replicationSlotLsn, currentTime);
            LOG.info("Emitted watermark: {} | Current LSN: {} | Slot LSN: {} | Lag: {}ms",
                    watermark, currentLsn, replicationSlotLsn, lag);
        }

        // Periodically cleanup old entries from the map to prevent unbounded growth
        periodicEmitCount++;
        if (periodicEmitCount >= MAP_CLEANUP_THRESHOLD) {
            cleanupOldLsnEntries();
            periodicEmitCount = 0;
        }
    }

    /**
     * Stub method for querying current PostgreSQL WAL LSN.
     *
     * <p>In a real implementation, this would execute:
     * <pre>
     * SELECT pg_current_wal_lsn();
     * </pre>
     *
     * @return Mock LSN value for demonstration
     */
    private String getCurrentLsn() {
        // Stub: simulate advancing LSN over time
        long simulatedLsn = System.currentTimeMillis() / 1000; // Changes every second
        return String.format("0/%08X", simulatedLsn);
    }

    /**
     * Stub method for querying PostgreSQL replication slot LSN.
     *
     * <p>In a real implementation, this would execute:
     * <pre>
     * SELECT confirmed_flush_lsn
     * FROM pg_replication_slots
     * WHERE slot_name = 'flink_slot' AND slot_type = 'logical';
     * </pre>
     *
     * @return Mock LSN value that lags behind current LSN
     */
    private String getReplicationSlotLsn() {
        // Stub: simulate replication lag of ~5 seconds
        long simulatedLagSeconds = 5;
        long simulatedLsn = (System.currentTimeMillis() / 1000) - simulatedLagSeconds;
        return String.format("0/%08X", simulatedLsn);
    }

    /**
     * Calculate watermark based on LSN-to-timestamp mapping using numeric LSN comparison.
     *
     * <p>LSN is a 64-bit integer representing a byte position in the WAL stream.
     * We compare LSNs numerically to find the most recent observation that is
     * less than or equal to the replication slot's LSN.
     *
     * <p>Logic:
     * <ol>
     *   <li>Parse current LSN and slot LSN to numeric values</li>
     *   <li>Find the most recent LSN observation <= slot LSN in our map</li>
     *   <li>Use that timestamp as the watermark (all events up to that time are replicated)</li>
     *   <li>If no suitable LSN found: fall back to default delay</li>
     * </ol>
     *
     * @return Calculated watermark timestamp
     */
    private long calculateWatermark() {
        long slotLsnNumeric = parseLsn(replicationSlotLsn);

        // Find the most recent LSN observation that is <= slot LSN
        long watermarkTimestamp = Long.MIN_VALUE;
        String bestMatchLsn = null;

        for (Map.Entry<String, Long> entry : lsnToTimestampMap.entrySet()) {
            long observedLsnNumeric = parseLsn(entry.getKey());

            // We want the highest LSN that is still <= slot LSN
            if (observedLsnNumeric <= slotLsnNumeric) {
                if (entry.getValue() > watermarkTimestamp) {
                    watermarkTimestamp = entry.getValue();
                    bestMatchLsn = entry.getKey();
                }
            }
        }

        if (watermarkTimestamp != Long.MIN_VALUE) {
            // Found a suitable LSN observation
            long currentLsnNumeric = parseLsn(currentLsn);
            long lsnLagBytes = currentLsnNumeric - slotLsnNumeric;

            LOG.debug("Found LSN observation {} <= slot LSN {}. Watermark timestamp: {}, LSN lag: {} bytes",
                    bestMatchLsn, replicationSlotLsn, watermarkTimestamp, lsnLagBytes);
            return watermarkTimestamp;
        } else {
            // No suitable LSN found in map - fall back to default delay
            long watermark = currentMaxTimestamp - DEFAULT_WATERMARK_DELAY_MS;
            LOG.debug("No LSN observation <= slot LSN {} found in map, using default delay. Watermark: {}",
                    replicationSlotLsn, watermark);
            return watermark;
        }
    }

    /**
     * Parse LSN string to numeric 64-bit value.
     *
     * <p>LSN format: Two hexadecimal numbers of up to 8 digits each, separated by a slash.
     * Example: "16/B374D848" = (0x16 << 32) | 0xB374D848
     *
     * @param lsn LSN string in format "XXXXXXXX/XXXXXXXX"
     * @return 64-bit numeric representation of the LSN
     */
    private long parseLsn(String lsn) {
        String[] parts = lsn.split("/");
        long upper = Long.parseLong(parts[0], 16);
        long lower = Long.parseLong(parts[1], 16);
        return (upper << 32) | lower;
    }

    /**
     * Remove old LSN entries from the map to prevent unbounded memory growth.
     *
     * <p>Keeps only the most recent entries that are still relevant for calculating lag.
     * In a real implementation, you might keep entries for the last N minutes or hours
     * depending on expected maximum replication lag.
     */
    private void cleanupOldLsnEntries() {
        long currentTime = System.currentTimeMillis();
        long retentionThresholdMs = 60_000; // Keep last 10 minutes

        Iterator<Map.Entry<String, Long>> iterator = lsnToTimestampMap.entrySet().iterator();
        int removedCount = 0;

        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            if (currentTime - entry.getValue() > retentionThresholdMs) {
                iterator.remove();
                removedCount++;
            }
        }

        if (removedCount > 0) {
            LOG.debug("Cleaned up {} old LSN entries from map. Remaining entries: {}",
                    removedCount, lsnToTimestampMap.size());
        }
    }
}
