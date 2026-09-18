package com.assignment.orders.producer.service;

import com.assignment.orders.avro.Order;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "app.generator.enabled", havingValue = "true")
public class OrderGenerator {
    private static final List<String> PRODUCTS = List.of("Item1", "Item2", "Item3", "Item4");
    private final OrderPublisher publisher;

    public OrderGenerator(OrderPublisher publisher) {
        this.publisher = publisher;
    }

    @Scheduled(fixedDelayString = "${app.generator.interval-ms:1000}")
    public void generate() {
        var random = ThreadLocalRandom.current();
        var order = Order.newBuilder()
                .setOrderId(UUID.randomUUID().toString())
                .setProduct(PRODUCTS.get(random.nextInt(PRODUCTS.size())))
                .setPrice((float) (Math.round(random.nextDouble(10.0, 500.0) * 100.0) / 100.0))
                .build();
        publisher.publish(order);
    }
}

