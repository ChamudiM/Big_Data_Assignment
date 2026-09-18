package com.assignment.orders.processor.error;

public class PermanentProcessingException extends RuntimeException {
    public PermanentProcessingException(String message) {
        super(message);
    }
}

