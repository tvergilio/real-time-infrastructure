package com.xdesign.flink.processing;

import com.xdesign.flink.model.SlackMessage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class SentimentAccumulatorTest {

    @Test
    void testAdd() {
        var accumulator = new SentimentAccumulator();
        var message = new SlackMessage(1721903155L, "U07DET2KZ2B", "Fantastic!");

        accumulator.add(message, new Tuple2<>(Arrays.asList(3), Arrays.asList("Positive")), 0, 0);

        assertEquals(1, accumulator.getCount());
        assertEquals(3.0, accumulator.getAverageScore(), 0.01);
        assertEquals("Positive", accumulator.getAverageClass());
        assertEquals("Fantastic!", accumulator.getMostPositiveMessage().getMessage());
    }

    @Test
    void testMerge() {
        var accumulator1 = new SentimentAccumulator();
        var accumulator2 = new SentimentAccumulator();

        var message1 = new SlackMessage(1721903155L, "U07DET2KZ4P", "Fantastic!");
        var message2 = new SlackMessage(1721903155L, "U07DET2KZ4P", "Awful!");

        accumulator1.add(message1, new Tuple2<>(Arrays.asList(3), Arrays.asList("Positive")), 0, 0);
        accumulator2.add(message2, new Tuple2<>(Arrays.asList(1), Arrays.asList("Negative")), 0, 0);

        accumulator1.merge(accumulator2);

        assertEquals(2, accumulator1.getCount());
        assertEquals(2.0, accumulator1.getAverageScore(), 0.01);
        assertEquals("Neutral", accumulator1.getAverageClass());
        assertEquals("Fantastic!", accumulator1.getMostPositiveMessage().getMessage());
        assertEquals("Awful!", accumulator1.getMostNegativeMessage().getMessage());
    }
}
