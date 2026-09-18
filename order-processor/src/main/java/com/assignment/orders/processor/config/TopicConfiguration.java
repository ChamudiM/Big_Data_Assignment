package com.assignment.orders.processor.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfiguration {
    @Bean
    NewTopic rawOrders(@Value("${app.topics.orders-raw}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic validatedOrders(@Value("${app.topics.orders-validated}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic deadLetterOrders(@Value("${app.topics.orders-dlt}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic priceAverages(@Value("${app.topics.price-averages}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1)
                .config("cleanup.policy", "compact")
                .build();
    }
}

