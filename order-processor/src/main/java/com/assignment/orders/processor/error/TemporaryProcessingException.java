package com.assignment.orders.processor.error;

public class TemporaryProcessingException extends RuntimeException {
    public TemporaryProcessingException(String message) {
        super(message);
    }
}

