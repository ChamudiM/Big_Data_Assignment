package com.assignment.orders.producer.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CreateOrderRequest(
        String orderId,
        @NotBlank String product,
        @NotNull Float price) {
}

