package com.assignment.orders.producer.api;

import com.assignment.orders.avro.Order;
import com.assignment.orders.producer.service.OrderPublisher;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrderControllerTest {
    @Test
    void createsAnIdWhenRequestDoesNotProvideOne() {
        var publisher = mock(OrderPublisher.class);
        when(publisher.publish(any(Order.class))).thenAnswer(invocation ->
                CompletableFuture.completedFuture(invocation.getArgument(0)));

        var response = new OrderController(publisher)
                .create(new CreateOrderRequest(null, "Item1", 25.5f))
                .join();

        assertThat(response.orderId()).isNotBlank();
        assertThat(response.product()).isEqualTo("Item1");
        assertThat(response.status()).isEqualTo("PUBLISHED");
    }
}
