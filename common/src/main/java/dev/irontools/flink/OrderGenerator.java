package dev.irontools.flink;

import com.github.javafaker.Faker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class OrderGenerator {
    private static final Faker faker = new Faker();
    private static final Random random = new Random();
    private static final String[] CATEGORIES = {
        "Electronics",
        "Clothing",
        "Books",
        "Home & Garden",
        "Sports"
    };
    private static final String[] PRODUCT_NAMES = {
        "Laptop",
        "Mouse",
        "Keyboard",
        "Monitor",
        "Headphones"
    };
    private static final String[] CUSTOMER_NAMES = {
        "Alice Johnson",
        "Bob Smith",
        "Charlie Brown",
        "Diana Prince",
        "Ethan Hunt"
    };

    public static List<Order> generateOrders(int count, boolean useStaticCustomerNames) {
        List<Order> orders = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            orders.add(generateOrder(useStaticCustomerNames));
        }
        return orders;
    }

    public static Order generateOrder(boolean useStaticCustomerNames) {
        String orderId = faker.idNumber().valid();
        String customerName = useStaticCustomerNames
            ? CUSTOMER_NAMES[random.nextInt(CUSTOMER_NAMES.length)]
            : faker.name().fullName();
        String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
        Double amount = 10.0 + (random.nextDouble() * 990.0);
        int productIndex = random.nextInt(PRODUCT_NAMES.length);
        String productId = "P" + (productIndex + 1);
        String productName = PRODUCT_NAMES[productIndex];
        Long timestamp = System.currentTimeMillis();

        return new Order(orderId, customerName, category, amount, productId, productName, timestamp);
    }

    /**
     * Generate an iterable of orders with delays between batches.
     * This is useful for simulating a streaming source that produces data over time.
     *
     * @param totalCount Total number of orders to generate
     * @param batchSize Number of orders to generate in each batch
     * @param delayMillis Delay in milliseconds between batches
     * @param useStaticCustomerNames Use static predefined customer names instead of Faker
     * @return Iterable of orders
     */
    public static Iterable<Order> generateOrdersWithDelay(int totalCount, int batchSize, long delayMillis, boolean useStaticCustomerNames) {
        return () -> new DelayedOrderIterator(totalCount, batchSize, delayMillis, useStaticCustomerNames);
    }

    /**
     * Iterator that generates orders with delays between batches.
     */
    public static class DelayedOrderIterator implements Iterator<Order>, Serializable {
        private final int totalCount;
        private final int batchSize;
        private final long delayMillis;
        private final boolean useStaticCustomerNames;
        private int generatedCount = 0;
        private int currentBatchCount = 0;

        public DelayedOrderIterator(int totalCount, int batchSize, long delayMillis, boolean useStaticCustomerNames) {
            this.totalCount = totalCount;
            this.batchSize = batchSize;
            this.delayMillis = delayMillis;
            this.useStaticCustomerNames = useStaticCustomerNames;
        }

        @Override
        public boolean hasNext() {
            return generatedCount < totalCount;
        }

        @Override
        public Order next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }

            // Pause between batches (but not before the first batch)
            if (currentBatchCount == 0 && generatedCount > 0) {
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while pausing", e);
                }
            }

            Order order = generateOrder(useStaticCustomerNames);
            generatedCount++;
            currentBatchCount++;

            // Reset batch counter when batch is complete
            if (currentBatchCount >= batchSize) {
                currentBatchCount = 0;
            }

            return order;
        }
    }
}
