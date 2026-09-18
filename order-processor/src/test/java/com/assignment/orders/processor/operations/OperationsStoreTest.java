package com.assignment.orders.processor.operations;

import com.assignment.orders.avro.Order;
import com.assignment.orders.avro.PriceAverage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class OperationsStoreTest {
    @Test
    void exposesOperationalStateAndCounters() {
        OperationsStore store = new OperationsStore();
        Instant now = Instant.parse("2026-01-01T12:00:00Z");
        store.updateAverage(PriceAverage.newBuilder().setProduct("books")
                .setAveragePrice(42.5).setTotalPrice(170.0).setOrderCount(4).setUpdatedAt(now).build());
        store.recordRetry(new ConsumerRecord<>("orders.raw", 1, 9, "o-1", "payload"),
                new IllegalStateException("temporary"), 2);
        Order order = Order.newBuilder().setOrderId("o-2").setProduct("books").setPrice(42.5f).build();
        store.recordDeadLetter(order, "orders.raw", "permanent", 0, 10);
        store.orderProcessed();
        store.replayed("o-2");

        OperationsStore.OperationsSummary summary = store.snapshot();
        assertThat(summary.processedOrders()).isEqualTo(1);
        assertThat(summary.replayedOrders()).isEqualTo(1);
        assertThat(summary.averages()).singleElement()
                .satisfies(view -> assertThat(view.product()).isEqualTo("books"));
        assertThat(summary.retries()).singleElement()
                .satisfies(view -> assertThat(view.attempt()).isEqualTo(2));
        assertThat(summary.deadLetters()).isEmpty();
    }
}
