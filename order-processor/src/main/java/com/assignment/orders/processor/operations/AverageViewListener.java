package com.assignment.orders.processor.operations;

import com.assignment.orders.avro.PriceAverage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AverageViewListener {
    private final OperationsStore store;

    public AverageViewListener(OperationsStore store) {
        this.store = store;
    }

    @KafkaListener(topics = "${app.topics.price-averages}", groupId = "operations-average-view-v2")
    public void update(PriceAverage average) {
        store.updateAverage(average);
    }
}

