package com.xdesign.flink.job;

import com.xdesign.flink.processing.RelevanceAggregate;
import com.xdesign.flink.processing.RelevanceScoringFunction;
import com.xdesign.flink.model.RestaurantEvent;
import com.xdesign.flink.sink.RedisSink;
import com.xdesign.flink.transfer.RestaurantEventDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Objects;
import java.util.Properties;

/**
 * A Flink job that calculates the relevance of restaurants based on views and likes.
 */
public class RestaurantRelevanceJob {

    public static void main(String[] args) throws Exception {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "flink-group");
        run(env, properties);
    }
    public static void run(StreamExecutionEnvironment env, Properties kafkaProperties) throws Exception {

        var viewsConsumer = new FlinkKafkaConsumer<>("restaurant_views", new RestaurantEventDeserializationSchema(), kafkaProperties);
        var likesConsumer = new FlinkKafkaConsumer<>("restaurant_likes", new RestaurantEventDeserializationSchema(), kafkaProperties);

        var viewsStream = env.addSource(viewsConsumer);
        var likesStream = env.addSource(likesConsumer);

        var eventsStream = viewsStream.union(likesStream)
                .filter(Objects::nonNull);

        // Apply windowing and aggregation to calculate relevance scores
        var  jsonStream = eventsStream.keyBy(RestaurantEvent::getRestaurantId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .aggregate(new RelevanceAggregate(), new RelevanceScoringFunction())
                .startNewChain()
                .name("Window/Aggregate/Score")
                .uid("score")

                // Convert the RestaurantRelevance object to JSON
                .map(new ObjectMapper()::writeValueAsString)
                .startNewChain()
                .name("Convert to JSON")
                .uid("json");

        // Separate streams for Kafka and Redis sinks
        var kafkaStream = jsonStream.map(value -> value)
                .startNewChain()
                .name("Kafka Stream")
                .uid("kafka-stream");
        var redisStream = jsonStream.map(value -> value)
                .startNewChain()
                .name("Redis Stream")
                .uid("redis-stream");

        // Sink the JSON to a Kafka topic
        kafkaStream.sinkTo(createKafkaSink(kafkaProperties.getProperty("bootstrap.servers")))
                .name("Sink to Kafka")
                .uid("sink-kafka");

        // Sink the JSON to Redis
        redisStream.sinkTo(new RedisSink("redis", 6379))
                .name("Sink to Redis")
                .uid("sink-redis");

        env.execute("Restaurant Relevance Job");
    }

    private static KafkaSink<String> createKafkaSink(String bootstrapServers) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers) // Replace with your Kafka brokers
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("restaurant_relevance")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
