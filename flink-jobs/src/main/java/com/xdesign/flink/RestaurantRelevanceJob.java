package com.xdesign.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.util.Properties;

public class RestaurantRelevanceJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "flink-group");

        FlinkKafkaConsumer<String> viewsConsumer = new FlinkKafkaConsumer<>("restaurant_views", new SimpleStringSchema(), properties);
        FlinkKafkaConsumer<String> likesConsumer = new FlinkKafkaConsumer<>("restaurant_likes", new SimpleStringSchema(), properties);

        DataStream<String> viewsStream = env.addSource(viewsConsumer);
        DataStream<String> likesStream = env.addSource(likesConsumer);

        DataStream<RestaurantEvent> eventsStream = viewsStream.union(likesStream).map(value -> {
            ObjectMapper mapper = new ObjectMapper();
            RestaurantEvent event = mapper.readValue(value, RestaurantEvent.class);
            System.out.println("Event: " + event);
            return event;
        });

        eventsStream.keyBy(RestaurantEvent::getRestaurantId)
                .timeWindow(Time.minutes(5))
                .aggregate(new RelevanceAggregate(), new RelevanceWindowingFunction())
                .addSink(createKafkaSink());

        env.execute("Restaurant Relevance Job");
    }

    private static FlinkKafkaProducer<RestaurantRelevance> createKafkaSink() {
        return new FlinkKafkaProducer<>(
                "kafka:9092",
                "restaurant_relevance",
                new KafkaSerialisationSchema()
        );
    }
}
