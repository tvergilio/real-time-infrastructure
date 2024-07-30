package com.xdesign.flink.processing;

import com.xdesign.flink.model.SlackMessage;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class SentimentAggregate implements AggregateFunction<
        Tuple2<SlackMessage, Tuple2<List<Integer>, List<String>>>,
        SentimentAccumulator,
        SentimentAccumulator> {

    @Override
    public SentimentAccumulator createAccumulator() {
        return new SentimentAccumulator();
    }

    @Override
    public SentimentAccumulator add(Tuple2<SlackMessage, Tuple2<List<Integer>, List<String>>> value, SentimentAccumulator accumulator) {
        accumulator.add(value.f0, value.f1, accumulator.getStart(), accumulator.getEnd());
        return accumulator;
    }

    @Override
    public SentimentAccumulator getResult(SentimentAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public SentimentAccumulator merge(SentimentAccumulator a, SentimentAccumulator b) {
        a.merge(b);
        return a;
    }
}
