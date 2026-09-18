package com.assignment.orders.processor.operations;

import com.assignment.orders.avro.Order;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/operations")
public class OperationsController {
    private final OperationsStore store;
    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final String rawTopic;

    public OperationsController(
            OperationsStore store,
            KafkaTemplate<String, Order> kafkaTemplate,
            @Value("${app.topics.orders-raw}") String rawTopic) {
        this.store = store;
        this.kafkaTemplate = kafkaTemplate;
        this.rawTopic = rawTopic;
    }

    @GetMapping("/summary")
    public OperationsStore.OperationsSummary summary() {
        return store.snapshot();
    }

    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishResponse publish(@Valid @RequestBody DemoOrderRequest request) {
        String orderId = request.orderId() == null || request.orderId().isBlank()
                ? UUID.randomUUID().toString()
                : request.orderId();
        var order = Order.newBuilder()
                .setOrderId(orderId)
                .setProduct(request.product())
                .setPrice(request.price())
                .build();
        kafkaTemplate.send(rawTopic, orderId, order).join();
        return new PublishResponse(orderId, "PUBLISHED");
    }

    @PostMapping("/dlq/{orderId}/replay")
    public PublishResponse replay(@PathVariable String orderId) {
        var failed = store.deadLetter(orderId)
                .orElseThrow(() -> new DeadLetterNotFoundException(orderId));
        var order = Order.newBuilder()
                .setOrderId(failed.orderId())
                .setProduct(failed.product())
                .setPrice(failed.price())
                .build();
        kafkaTemplate.send(rawTopic, orderId, order).join();
        store.replayed(orderId);
        return new PublishResponse(orderId, "REPLAYED");
    }

    public record DemoOrderRequest(String orderId, @NotBlank String product, @NotNull Float price) {}
    public record PublishResponse(String orderId, String status) {}

    @ResponseStatus(HttpStatus.NOT_FOUND)
    static class DeadLetterNotFoundException extends RuntimeException {
        DeadLetterNotFoundException(String orderId) {
            super("No DLQ order found with id " + orderId);
        }
    }
}
