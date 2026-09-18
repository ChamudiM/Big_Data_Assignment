package com.assignment.orders.processor.messaging;

import com.assignment.orders.avro.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import com.assignment.orders.processor.operations.OperationsStore;

import static org.springframework.kafka.support.KafkaHeaders.DLT_EXCEPTION_MESSAGE;
import static org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_TOPIC;

@Component
public class DeadLetterListener {
    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);
    private final OperationsStore operationsStore;

    public DeadLetterListener(OperationsStore operationsStore) {
        this.operationsStore = operationsStore;
    }

    @KafkaListener(topics = "${app.topics.orders-dlt}", groupId = "order-dlt-monitor")
    public void monitor(
            ConsumerRecord<String, Order> record,
            @Header(name = DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
            @Header(name = DLT_EXCEPTION_MESSAGE, required = false) String errorMessage) {
        log.error("DLQ orderId={} originalTopic={} partition={} offset={} reason={}",
                record.value().getOrderId(), originalTopic, record.partition(), record.offset(), errorMessage);
        operationsStore.recordDeadLetter(
                record.value(), originalTopic, errorMessage, record.partition(), record.offset());
    }
}
