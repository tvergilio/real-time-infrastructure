package com.xdesign.flink.processing;

import com.xdesign.flink.model.SlackMessage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

class SentimentAggregateTest {

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
        assertEquals("Neutral", result.getResult());
        assertEquals("Fantastic!", result.getMostPositiveMessage());
        assertEquals("Awful!", result.getMostNegativeMessage());
    }
}
