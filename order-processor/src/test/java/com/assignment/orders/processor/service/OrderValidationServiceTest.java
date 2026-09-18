package com.assignment.orders.processor.service;

import com.assignment.orders.avro.Order;
import com.assignment.orders.processor.error.PermanentProcessingException;
import com.assignment.orders.processor.error.TemporaryProcessingException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderValidationServiceTest {
    private final OrderValidationService service = new OrderValidationService();

    @Test
    void acceptsValidOrder() {
        assertThatCode(() -> service.validateAndProcess(order("1", "Item1", 10f)))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsInvalidPriceWithoutRetry() {
        assertThatThrownBy(() -> service.validateAndProcess(order("2", "Item1", -1f)))
                .isInstanceOf(PermanentProcessingException.class);
    }

    @Test
    void temporaryDemoFailureSucceedsOnThirdAttempt() {
        var order = order("3", OrderValidationService.TEMPORARY_FAILURE_PRODUCT, 12f);
        assertThatThrownBy(() -> service.validateAndProcess(order))
                .isInstanceOf(TemporaryProcessingException.class);
        assertThatThrownBy(() -> service.validateAndProcess(order))
                .isInstanceOf(TemporaryProcessingException.class);
        assertThatCode(() -> service.validateAndProcess(order)).doesNotThrowAnyException();
    }

    private Order order(String id, String product, float price) {
        return Order.newBuilder().setOrderId(id).setProduct(product).setPrice(price).build();
    }
}

