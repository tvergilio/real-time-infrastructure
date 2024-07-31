package com.xdesign.flink.job;

import com.xdesign.flink.processing.*;
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
 * A Flink job that reads Slack messages from a Kafka topic, performs sentiment analysis using the Stanford NLP model and
 * GPT-4, and writes the results to different Kafka topics.
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

        // Perform sentiment analysis using the Stanford NLP model
        var stanfordSentimentResultsStream = slackMessagesStream.map(new StanfordSentimentAnalysisFunction())
                .startNewChain()
                .name("Map: Stanford Sentiment Analysis")
                .uid("map-stanford-sentiment-analysis");

        // Apply sliding windows of 1 minute with slides of 30 seconds
        var stanfordWindowedStream = stanfordSentimentResultsStream
                .windowAll(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .aggregate(new StanfordSentimentAggregator(), new ProcessAllWindowFunction<StanfordSentimentAccumulator, StanfordSentimentAccumulator, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<StanfordSentimentAccumulator> elements, Collector<StanfordSentimentAccumulator> out) {
                        StanfordSentimentAccumulator accumulator = elements.iterator().next();
                        accumulator.setStart(context.window().getStart());
                        accumulator.setEnd(context.window().getEnd());
                        out.collect(accumulator);
                    }
                })
                .startNewChain()
                .name("Aggregate: Stanford Windowed Sentiment Analysis")
                .uid("aggregate-stanford-windowed-sentiment-analysis");

        // Perform sentiment analysis using the GPT-4 LLM model
        var gpt4WindowedStream = slackMessagesStream
                .windowAll(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .aggregate(new SlackMessageAggregator(), new GPT4WindowProcessingFunction(new GPT4ProcessingFunction()))
                .name("Aggregate: GPT-4 Windowed Sentiment Analysis")
                .uid("aggregate-gpt4-windowed-sentiment-analysis");

        // Convert the results to JSON format
        var stanfordJsonResultsStream = stanfordWindowedStream.map(StanfordSentimentAccumulator::toString)
                .startNewChain()
                .name("Map: Convert Stanford to JSON")
                .uid("map-convert-stanford-to-json");

        var gpt4JsonResultsStream = gpt4WindowedStream.map(GPT4SentimentAccumulator::toString)
                .startNewChain()
                .name("Map: Convert GPT-4 to JSON")
                .uid("map-convert-gpt4-to-json");

        // Sink the results to different Kafka topics
        stanfordJsonResultsStream.sinkTo(createKafkaSink(kafkaProperties.getProperty("bootstrap.servers"), "stanford_results"))
                .name("Kafka Sink: Stanford Sentiment Results")
                .uid("kafka-sink-stanford-sentiment-results");

        gpt4JsonResultsStream.sinkTo(createKafkaSink(kafkaProperties.getProperty("bootstrap.servers"), "gpt4_results"))
                .name("Kafka Sink: GPT-4 Sentiment Results")
                .uid("kafka-sink-gpt4-sentiment-results");

        env.execute("Sentiment Comparison Job");
    }

    private static KafkaSink<String> createKafkaSink(String bootstrapServers, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
