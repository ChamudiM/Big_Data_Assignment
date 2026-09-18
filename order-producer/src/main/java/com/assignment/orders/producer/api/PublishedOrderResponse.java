package com.assignment.orders.producer.api;

public record PublishedOrderResponse(
        String orderId,
        String product,
        float price,
        String status) {
}

