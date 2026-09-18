package com.assignment.orders.producer.service;

import com.assignment.orders.avro.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderPublisher {
    private static final Logger log = LoggerFactory.getLogger(OrderPublisher.class);

    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final String ordersTopic;

    public OrderPublisher(
            KafkaTemplate<String, Order> kafkaTemplate,
            @Value("${app.topics.orders-raw}") String ordersTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.ordersTopic = ordersTopic;
    }

    public CompletableFuture<Order> publish(Order order) {
        return kafkaTemplate.send(ordersTopic, order.getOrderId(), order)
                .thenApply(result -> {
                    var metadata = result.getRecordMetadata();
                    log.info("Published orderId={} product={} partition={} offset={}",
                            order.getOrderId(), order.getProduct(), metadata.partition(), metadata.offset());
                    return order;
                });
    }
}

