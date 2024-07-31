package com.xdesign.flink.processing;

import com.xdesign.flink.model.SlackMessage;
import com.xdesign.flink.transfer.SlackMessageDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StanfordSentimentAnalysisFunctionTest {

    private StanfordSentimentAnalysisFunction function;

    @BeforeEach
    void setUp() {
        function = new StanfordSentimentAnalysisFunction();
        function.open(new Configuration());
    }

    @Test
    void testMapVeryPositive() throws Exception {
        var message = new SlackMessage(1721903155L, "U07DET2KZ2B", "Fantastic!");
        var result = function.map(message);

        assertNotNull(result);
        assertEquals(message, result.f0);
        assertEquals(1, result.f1.f0.size());
        assertEquals("Very positive", result.f1.f1.get(0));
    }

    @Test
    void testMapPositive() throws Exception {
        var message = new SlackMessage(1721903155L, "U07DET2KZ2B", "Good job!");
        var result = function.map(message);

        assertNotNull(result);
        assertEquals(message, result.f0);
        assertEquals(1, result.f1.f0.size());
        assertEquals("Positive", result.f1.f1.get(0));
    }

    @Test
    void testMapNeutral() throws Exception {
        var message = new SlackMessage(1721903155L, "U07DET2KZ2B", "It's so-so.");
        var result = function.map(message);

        assertNotNull(result);
        assertEquals(message, result.f0);
        assertEquals(1, result.f1.f0.size());
        assertEquals("Neutral", result.f1.f1.get(0));
    }

    @Test
    void testMapNegative() throws Exception {
        var message = new SlackMessage(1721903155L, "U07DET2KZ2B", "This is bad.");
        var result = function.map(message);

        assertNotNull(result);
        assertEquals(message, result.f0);
        assertEquals(1, result.f1.f0.size());
        assertEquals("Negative", result.f1.f1.get(0));
    }

    @Test
    void testMapVeryNegative() throws Exception {
        var message = new SlackMessage(1721903155L, "U07DET2KZ2B", "Terrible!");
        var result = function.map(message);

        assertNotNull(result);
        assertEquals(message, result.f0);
        assertEquals(1, result.f1.f0.size());
        assertEquals("Very negative", result.f1.f1.get(0));
    }

    @Test
    void testDeserialize() throws Exception {
        var input = "Timestamp: 1721903155.837829, User: U07DET2KZ2B, Message: Fantastic!";
        var schema = new SlackMessageDeserializationSchema();
        var message = schema.deserialize(input.getBytes());

        assertEquals(1721903155L, message.getTimestamp());
        assertEquals("U07DET2KZ2B", message.getUser());
        assertEquals("Fantastic!", message.getMessage());
    }

    @Test
    void testAddAndMerge() {
        var aggregate = new StanfordSentimentAggregator();
        var accumulator1 = new StanfordSentimentAccumulator();
        var accumulator2 = new StanfordSentimentAccumulator();

        var message1 = new SlackMessage(1721903155L, "U07DET2KZ2B", "Fantastic!");
        var message2 = new SlackMessage(1721903155L, "U07DET2KZ2B", "Awful!");

        accumulator1.add(message1, new Tuple2<>(List.of(3), List.of("Positive")));
        accumulator2.add(message2, new Tuple2<>(List.of(1), List.of("Negative")));

        var result = aggregate.merge(accumulator1, accumulator2);

        assertEquals(2, result.getMessageCount());
        assertEquals(2.0, result.getAverageScore(), 0.01);
        assertEquals("Neutral", result.getResult()); // Changed to "Neutral"
        assertEquals("Fantastic!", result.getMostPositiveMessage());
        assertEquals("Awful!", result.getMostNegativeMessage());
    }

    @Test
    void testAddToAccumulator() {
        var accumulator = new StanfordSentimentAccumulator();
        var message = new SlackMessage(1721903155L, "U07DET2KZ2B", "Fantastic!");

        accumulator.add(message, new Tuple2<>(List.of(3), List.of("Positive")));

        assertEquals(1, accumulator.getMessageCount());
        assertEquals(3.0, accumulator.getAverageScore(), 0.01);
        assertEquals("Positive", accumulator.getResult());
        assertEquals("Fantastic!", accumulator.getMostPositiveMessage());
    }
}
