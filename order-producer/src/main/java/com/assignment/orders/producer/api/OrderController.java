package com.assignment.orders.producer.api;

import com.assignment.orders.avro.Order;
import com.assignment.orders.producer.service.OrderPublisher;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/orders")
public class OrderController {
    private final OrderPublisher publisher;

    public OrderController(OrderPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CompletableFuture<PublishedOrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
        String orderId = request.orderId() == null || request.orderId().isBlank()
                ? UUID.randomUUID().toString()
                : request.orderId();
        var order = Order.newBuilder()
                .setOrderId(orderId)
                .setProduct(request.product())
                .setPrice(request.price())
                .build();
        return publisher.publish(order)
                .thenApply(published -> new PublishedOrderResponse(
                        published.getOrderId(), published.getProduct(), published.getPrice(), "PUBLISHED"));
    }
}

