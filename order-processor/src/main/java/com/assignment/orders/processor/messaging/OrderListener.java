package com.assignment.orders.processor.messaging;

import com.assignment.orders.avro.Order;
import com.assignment.orders.processor.service.OrderValidationService;
import com.assignment.orders.processor.operations.OperationsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderListener {
    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    private final OrderValidationService validationService;
    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final String validatedTopic;
    private final OperationsStore operationsStore;

    public OrderListener(
            OrderValidationService validationService,
            KafkaTemplate<String, Order> kafkaTemplate,
            OperationsStore operationsStore,
            @Value("${app.topics.orders-validated}") String validatedTopic) {
        this.validationService = validationService;
        this.kafkaTemplate = kafkaTemplate;
        this.operationsStore = operationsStore;
        this.validatedTopic = validatedTopic;
    }

    @KafkaListener(topics = "${app.topics.orders-raw}", groupId = "${app.consumer.group-id}")
    public void consume(Order order) {
        log.info("Processing orderId={} product={} price={}",
                order.getOrderId(), order.getProduct(), order.getPrice());
        validationService.validateAndProcess(order);
        kafkaTemplate.send(validatedTopic, order.getProduct(), order).join();
        operationsStore.orderProcessed();
        log.info("Validated orderId={} and forwarded to {}", order.getOrderId(), validatedTopic);
    }
}
