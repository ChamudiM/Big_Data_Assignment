package com.assignment.orders.processor.stream;

import com.assignment.orders.avro.Order;
import com.assignment.orders.avro.PriceAverage;
import com.assignment.orders.avro.PriceStatistics;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;
import java.util.Map;

@Configuration
public class PriceAverageTopology {
    @Bean
    public KStream<String, PriceAverage> priceAverageStream(
            StreamsBuilder builder,
            @Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl,
            @Value("${app.topics.orders-validated}") String inputTopic,
            @Value("${app.topics.price-averages}") String outputTopic) {

        Map<String, String> serdeConfig = Map.of("schema.registry.url", schemaRegistryUrl);
        var orderSerde = new SpecificAvroSerde<Order>();
        orderSerde.configure(serdeConfig, false);
        var statisticsSerde = new SpecificAvroSerde<PriceStatistics>();
        statisticsSerde.configure(serdeConfig, false);
        var averageSerde = new SpecificAvroSerde<PriceAverage>();
        averageSerde.configure(serdeConfig, false);

        KStream<String, PriceAverage> averages = builder.stream(inputTopic, Consumed.with(Serdes.String(), orderSerde))
                .selectKey((ignored, order) -> order.getProduct())
                .groupByKey(Grouped.with(Serdes.String(), orderSerde))
                .aggregate(
                        () -> PriceStatistics.newBuilder().setCount(0L).setTotal(0.0).build(),
                        (product, order, current) -> PriceStatistics.newBuilder()
                                .setCount(current.getCount() + 1)
                                .setTotal(current.getTotal() + order.getPrice())
                                .build(),
                        Materialized.with(Serdes.String(), statisticsSerde))
                .toStream()
                .mapValues((product, statistics) -> PriceAverage.newBuilder()
                        .setProduct(product)
                        .setOrderCount(statistics.getCount())
                        .setTotalPrice(statistics.getTotal())
                        .setAveragePrice(statistics.getTotal() / statistics.getCount())
                        .setUpdatedAt(Instant.now())
                        .build());

        averages.to(outputTopic, Produced.with(Serdes.String(), averageSerde));
        return averages;
    }
}
