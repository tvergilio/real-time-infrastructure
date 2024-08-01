package com.xdesign.flink.processing;

import com.xdesign.flink.model.SlackMessage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SentimentAccumulatorTest {

    @Test
    void testAdd() {
        var accumulator = new StanfordSentimentAccumulator();
        var message = new SlackMessage(1721903155L, "U07DET2KZ2B", "Fantastic!");

        accumulator.add(message, new Tuple2<>(List.of(3), List.of("Positive")));

        assertEquals(1, accumulator.getMessageCount());
        assertEquals(3.0, accumulator.getAverageScore(), 0.01);
        assertEquals("Positive", accumulator.getResult());
        assertEquals("Fantastic!", accumulator.getMostPositiveMessage());
    }

    @Test
    void testMerge() {
        var accumulator1 = new StanfordSentimentAccumulator();
        var accumulator2 = new StanfordSentimentAccumulator();

        var message1 = new SlackMessage(1721903155L, "U07DET2KZ4P", "Fantastic!");
        var message2 = new SlackMessage(1721903155L, "U07DET2KZ4P", "Awful!");

        accumulator1.add(message1, new Tuple2<>(List.of(3), List.of("Positive")));
        accumulator2.add(message2, new Tuple2<>(List.of(1), List.of("Negative")));

        accumulator1.merge(accumulator2);

        assertEquals(2, accumulator1.getMessageCount());
        assertEquals(2.0, accumulator1.getAverageScore(), 0.01);
        assertEquals("Neutral", accumulator1.getResult());
        assertEquals("Fantastic!", accumulator1.getMostPositiveMessage());
        assertEquals("Awful!", accumulator1.getMostNegativeMessage());
    }

    @Test
    void testAddWithSameScoreDifferentLengths() {
        var accumulator = new StanfordSentimentAccumulator();

        var message1 = new SlackMessage(1721903155L, "U07DET2KZ4P", "Good");
        var message2 = new SlackMessage(1721903155L, "U07DET2KZ4P", "A much longer message");

        accumulator.add(message1, new Tuple2<>(List.of(2), List.of("Neutral")));
        accumulator.add(message2, new Tuple2<>(List.of(2), List.of("Neutral")));

        assertEquals("A much longer message", accumulator.getMostPositiveMessage());
        assertEquals("Good", accumulator.getMostNegativeMessage());
    }

    @Test
    void testMergeWithSameScoreDifferentLengths() {
        var accumulator1 = new StanfordSentimentAccumulator();
        var accumulator2 = new StanfordSentimentAccumulator();

        var message1 = new SlackMessage(1721903155L, "U07DET2KZ4P", "Good message");
        var message2 = new SlackMessage(1721903155L, "U07DET2KZ4P", "Another good message");

        accumulator1.add(message1, new Tuple2<>(List.of(2), List.of("Neutral")));
        accumulator2.add(message2, new Tuple2<>(List.of(2), List.of("Neutral")));

        accumulator1.merge(accumulator2);

        assertEquals("Another good message", accumulator1.getMostPositiveMessage());
        assertEquals("Good message", accumulator1.getMostNegativeMessage());
    }
}
