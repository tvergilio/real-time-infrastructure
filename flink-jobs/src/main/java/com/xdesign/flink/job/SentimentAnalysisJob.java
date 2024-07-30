package com.xdesign.flink.job;

import com.xdesign.flink.processing.SentimentAggregate;
import com.xdesign.flink.processing.SentimentAccumulator;
import com.xdesign.flink.processing.SentimentAnalysisFunction;
import com.xdesign.flink.transfer.SlackMessageDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * A Flink job that reads messages from a Slack channel, performs sentiment analysis,
 * and writes the results to a Kafka topic.
 */
public class SentimentAnalysisJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        var properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "flink-group");
        run(env, properties);
    }

    public static void run(StreamExecutionEnvironment env, Properties kafkaProperties) throws Exception {
        // Consumer for reading Slack messages
        var slackMessagesConsumer = new FlinkKafkaConsumer<>("slack_messages", new SlackMessageDeserializationSchema(), kafkaProperties);
        var slackMessagesStream = env.addSource(slackMessagesConsumer)
                .name("Kafka Source: Slack Messages")
                .uid("kafka-source-slack-messages");

        // Perform sentiment analysis on the messages
        var sentimentResultsStream = slackMessagesStream.map(new SentimentAnalysisFunction())
                .startNewChain()
                .name("Map: Sentiment Analysis")
                .uid("map-sentiment-analysis");

        // Apply sliding windows of 1 minute with slides of 30 seconds
        var windowedStream = sentimentResultsStream
                .windowAll(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .aggregate(new SentimentAggregate(), new ProcessAllWindowFunction<SentimentAccumulator, SentimentAccumulator, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<SentimentAccumulator> elements, Collector<SentimentAccumulator> out) {
                        SentimentAccumulator accumulator = elements.iterator().next();
                        accumulator.setStart(context.window().getStart());
                        accumulator.setEnd(context.window().getEnd());
                        out.collect(accumulator);
                    }
                })
                .startNewChain()
                .name("Aggregate: Windowed Sentiment Analysis")
                .uid("aggregate-windowed-sentiment-analysis");

        // Convert the results to JSON format
        var jsonResultsStream = windowedStream.map(value -> value.toString())
                .startNewChain()
                .name("Map: Convert to JSON")
                .uid("map-convert-to-json");

        // Sink the results to a Kafka topic
        jsonResultsStream.sinkTo(createKafkaSink(kafkaProperties.getProperty("bootstrap.servers")))
                .name("Kafka Sink: Sentiment Results")
                .uid("kafka-sink-sentiment-results");

        env.execute("Sentiment Analysis Job");
    }

    private static KafkaSink<String> createKafkaSink(String bootstrapServers) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("sentiment_results")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
