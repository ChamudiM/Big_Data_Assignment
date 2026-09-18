package com.assignment.orders.processor.service;

import com.assignment.orders.avro.Order;
import com.assignment.orders.processor.error.PermanentProcessingException;
import com.assignment.orders.processor.error.TemporaryProcessingException;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class OrderValidationService {
    public static final String TEMPORARY_FAILURE_PRODUCT = "TEMP_FAIL";
    public static final String PERMANENT_FAILURE_PRODUCT = "PERM_FAIL";

    private final ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    public void validateAndProcess(Order order) {
        if (order.getOrderId() == null || order.getOrderId().isBlank()) {
            throw new PermanentProcessingException("orderId must not be blank");
        }
        if (order.getProduct() == null || order.getProduct().isBlank()) {
            throw new PermanentProcessingException("product must not be blank");
        }
        if (!Float.isFinite(order.getPrice()) || order.getPrice() <= 0) {
            throw new PermanentProcessingException("price must be a finite value greater than zero");
        }
        if (PERMANENT_FAILURE_PRODUCT.equalsIgnoreCase(order.getProduct())) {
            throw new PermanentProcessingException("demonstration permanent failure");
        }
        if (TEMPORARY_FAILURE_PRODUCT.equalsIgnoreCase(order.getProduct())) {
            int attempt = attempts.computeIfAbsent(order.getOrderId(), ignored -> new AtomicInteger())
                    .incrementAndGet();
            if (attempt <= 2) {
                throw new TemporaryProcessingException("demonstration temporary failure on attempt " + attempt);
            }
            attempts.remove(order.getOrderId());
        }
    }
}

