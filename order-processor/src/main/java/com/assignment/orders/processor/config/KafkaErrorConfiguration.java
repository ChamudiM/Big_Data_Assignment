package com.assignment.orders.processor.config;

import com.assignment.orders.processor.error.PermanentProcessingException;
import com.assignment.orders.processor.operations.OperationsStore;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaErrorConfiguration {
    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate,
            OperationsStore operationsStore,
            @Value("${app.topics.orders-dlt}") String dltTopic) {
        var recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(dltTopic, record.partition()));

        var backOff = new ExponentialBackOff(2_000L, 2.0);
        backOff.setMaxInterval(10_000L);
        backOff.setMaxAttempts(3);

        var errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(PermanentProcessingException.class);
        errorHandler.setRetryListeners((record, exception, deliveryAttempt) -> {
            operationsStore.recordRetry(record, exception, deliveryAttempt);
            org.slf4j.LoggerFactory.getLogger(KafkaErrorConfiguration.class).warn(
                        "Retry attempt={} topic={} partition={} offset={} reason={}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(), exception.getMessage());
        });
        return errorHandler;
    }
}
