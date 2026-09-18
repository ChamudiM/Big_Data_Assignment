package com.assignment.orders.processor.stream;

import com.assignment.orders.avro.Order;
import com.assignment.orders.avro.PriceAverage;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class PriceAverageTopologyTest {
    private static final String INPUT = "orders.validated";
    private static final String OUTPUT = "price-averages";
    private static final String SCHEMA_REGISTRY = "mock://price-average-test";

    @Test
    void calculatesRunningAverageByProduct() {
        var builder = new StreamsBuilder();
        new PriceAverageTopology().priceAverageStream(builder, SCHEMA_REGISTRY, INPUT, OUTPUT);

        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "price-average-topology-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        var orderSerde = new SpecificAvroSerde<Order>();
        orderSerde.configure(Map.of("schema.registry.url", SCHEMA_REGISTRY), false);
        var averageSerde = new SpecificAvroSerde<PriceAverage>();
        averageSerde.configure(Map.of("schema.registry.url", SCHEMA_REGISTRY), false);

        try (var driver = new TopologyTestDriver(builder.build(), properties)) {
            var input = driver.createInputTopic(
                    INPUT, Serdes.String().serializer(), orderSerde.serializer());
            var output = driver.createOutputTopic(
                    OUTPUT, Serdes.String().deserializer(), averageSerde.deserializer());

            input.pipeInput("1", order("1", "Item1", 100f));
            input.pipeInput("2", order("2", "Item1", 200f));
            input.pipeInput("3", order("3", "Item1", 300f));

            var records = output.readKeyValuesToList();
            var latest = records.getLast();
            assertThat(latest.key).isEqualTo("Item1");
            assertThat(latest.value.getOrderCount()).isEqualTo(3L);
            assertThat(latest.value.getTotalPrice()).isEqualTo(600.0);
            assertThat(latest.value.getAveragePrice()).isEqualTo(200.0);
        } finally {
            orderSerde.close();
            averageSerde.close();
        }
    }

    private Order order(String id, String product, float price) {
        return Order.newBuilder().setOrderId(id).setProduct(product).setPrice(price).build();
    }
}
