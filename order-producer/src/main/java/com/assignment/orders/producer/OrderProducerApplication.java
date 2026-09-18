package com.assignment.orders.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class OrderProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderProducerApplication.class, args);
    }
}

