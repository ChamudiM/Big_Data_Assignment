package com.assignment.orders.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafka
@EnableKafkaStreams
@SpringBootApplication
public class OrderProcessorApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderProcessorApplication.class, args);
    }
}

