package com.xdesign.flink.processing;

import com.xdesign.flink.model.SlackMessage;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class StanfordSentimentAggregator implements AggregateFunction<
        Tuple2<SlackMessage, Tuple2<List<Integer>, List<String>>>,
        StanfordSentimentAccumulator,
        StanfordSentimentAccumulator> {

    @Override
    public StanfordSentimentAccumulator createAccumulator() {
        return new StanfordSentimentAccumulator();
    }

    @Override
    public StanfordSentimentAccumulator add(Tuple2<SlackMessage, Tuple2<List<Integer>, List<String>>> value, StanfordSentimentAccumulator accumulator) {
        accumulator.add(value.f0, value.f1);
        return accumulator;
    }

    @Override
    public StanfordSentimentAccumulator getResult(StanfordSentimentAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public StanfordSentimentAccumulator merge(StanfordSentimentAccumulator a, StanfordSentimentAccumulator b) {
        a.merge(b);
        return a;
    }
}
