package dev.irontools.flink;

import com.github.javafaker.Faker;

import java.util.ArrayList;
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

    public static List<Order> generateOrders(int count) {
        List<Order> orders = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            orders.add(generateOrder());
        }
        return orders;
    }

    public static Order generateOrder() {
        String orderId = faker.idNumber().valid();
        String customerName = faker.name().fullName();
        String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
        Double amount = 10.0 + (random.nextDouble() * 990.0);
        String productName = faker.commerce().productName();

        return new Order(orderId, customerName, category, amount, productName);
    }
}
