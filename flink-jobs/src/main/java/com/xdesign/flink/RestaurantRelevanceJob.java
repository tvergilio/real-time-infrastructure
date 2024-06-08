package com.xdesign.flink;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class RestaurantRelevanceJob {
    public static void main(String[] args) throws Exception {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();

        var properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "flink-group");

        var viewsConsumer = new FlinkKafkaConsumer<>("restaurant_views", new RestaurantEventDeserializationSchema(), properties);
        var likesConsumer = new FlinkKafkaConsumer<>("restaurant_likes", new RestaurantEventDeserializationSchema(), properties);

        var viewsStream = env.addSource(viewsConsumer);
        var likesStream = env.addSource(likesConsumer);

        var eventsStream = viewsStream.union(likesStream);
        var eventsStreamWithTimestamps = eventsStream.assignTimestampsAndWatermarks(new RestaurantEventWatermarkStrategy())
                        .name("Assign Timestamp");

        eventsStreamWithTimestamps.keyBy(RestaurantEvent::getRestaurantId)
                .timeWindow(Time.seconds(10))
                .allowedLateness(Time.seconds(1))
                .aggregate(new RelevanceAggregate(), new RelevanceWindowingFunction())
                .name("Aggregate and Window")
                .map(new ObjectMapper()::writeValueAsString)
                .name("Convert to JSON")
                .sinkTo(createKafkaSink())
                .name("Sink to Kafka")
                .uid("Sink to Kafka");

        env.execute("Restaurant Relevance Job");
    }

    private static KafkaSink<String> createKafkaSink() {
        return KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092") // Replace with your Kafka brokers
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("restaurant_relevance")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
