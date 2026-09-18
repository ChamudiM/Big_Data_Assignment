package com.assignment.orders.processor.operations;

import com.assignment.orders.avro.Order;
import com.assignment.orders.avro.PriceAverage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class OperationsStore {
    private static final int MAX_EVENTS = 100;

    private final Map<String, AverageView> averages = new ConcurrentHashMap<>();
    private final Map<String, DeadLetterView> deadLetters = new ConcurrentHashMap<>();
    private final ArrayDeque<RetryView> retryEvents = new ArrayDeque<>();
    private final AtomicLong processedOrders = new AtomicLong();
    private final AtomicLong replayedOrders = new AtomicLong();

    public void updateAverage(PriceAverage average) {
        averages.put(average.getProduct(), new AverageView(
                average.getProduct(), average.getOrderCount(), average.getTotalPrice(),
                average.getAveragePrice(), average.getUpdatedAt()));
    }

    public void orderProcessed() {
        processedOrders.incrementAndGet();
    }

    public synchronized void recordRetry(ConsumerRecord<?, ?> record, Exception exception, int attempt) {
        String orderId = record.value() instanceof Order order ? order.getOrderId() : "unavailable";
        retryEvents.addFirst(new RetryView(
                orderId, attempt, exception.getClass().getSimpleName(), exception.getMessage(), Instant.now()));
        while (retryEvents.size() > MAX_EVENTS) {
            retryEvents.removeLast();
        }
    }

    public void recordDeadLetter(Order order, String originalTopic, String reason, int partition, long offset) {
        deadLetters.put(order.getOrderId(), new DeadLetterView(
                order.getOrderId(), order.getProduct(), order.getPrice(), originalTopic,
                reason, partition, offset, Instant.now()));
    }

    public Optional<DeadLetterView> deadLetter(String orderId) {
        return Optional.ofNullable(deadLetters.get(orderId));
    }

    public void replayed(String orderId) {
        deadLetters.remove(orderId);
        replayedOrders.incrementAndGet();
    }

    public OperationsSummary snapshot() {
        List<AverageView> averageViews = averages.values().stream()
                .sorted(Comparator.comparing(AverageView::product))
                .toList();
        List<DeadLetterView> dlqViews = deadLetters.values().stream()
                .sorted(Comparator.comparing(DeadLetterView::failedAt).reversed())
                .toList();
        List<RetryView> retries;
        synchronized (this) {
            retries = new ArrayList<>(retryEvents);
        }
        return new OperationsSummary(
                averageViews, dlqViews, retries, processedOrders.get(), replayedOrders.get(), Instant.now());
    }

    public record AverageView(
            String product, long orderCount, double totalPrice, double averagePrice, Instant updatedAt) {}

    public record DeadLetterView(
            String orderId, String product, float price, String originalTopic, String reason,
            int partition, long offset, Instant failedAt) {}

    public record RetryView(
            String orderId, int attempt, String exceptionType, String reason, Instant occurredAt) {}

    public record OperationsSummary(
            List<AverageView> averages,
            List<DeadLetterView> deadLetters,
            List<RetryView> retries,
            long processedOrders,
            long replayedOrders,
            Instant generatedAt) {}
}
