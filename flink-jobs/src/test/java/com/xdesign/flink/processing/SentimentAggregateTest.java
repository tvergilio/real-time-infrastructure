package com.xdesign.flink.processing;

import com.xdesign.flink.model.SlackMessage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class SentimentAggregateTest {

    @Test
    void testAddAndMerge() {
        var aggregate = new SentimentAggregate();
        var accumulator1 = new SentimentAccumulator();
        var accumulator2 = new SentimentAccumulator();

        var message1 = new SlackMessage(1721903155L, "U07DET2KZ2B", "Fantastic!");
        var message2 = new SlackMessage(1721903155L, "U07DET2KZ2B", "Awful!");

        accumulator1.add(message1, new Tuple2<>(Arrays.asList(3), Arrays.asList("Positive")), 0, 0);
        accumulator2.add(message2, new Tuple2<>(Arrays.asList(1), Arrays.asList("Negative")), 0, 0);

        var result = aggregate.merge(accumulator1, accumulator2);

        assertEquals(2, result.getCount());
        assertEquals(2, result.getAverageScore(), 0.01);
        assertEquals("Neutral", result.getAverageClass());
        assertEquals("Fantastic!", result.getMostPositiveMessage().getMessage());
        assertEquals("Awful!", result.getMostNegativeMessage().getMessage());
    }
}
