package com.xdesign.flink.processing;

import org.apache.flink.api.common.functions.AggregateFunction;
import com.xdesign.flink.model.SlackMessage;
import java.util.ArrayList;
import java.util.List;

public class SlackMessageAggregator implements AggregateFunction<SlackMessage, List<SlackMessage>, List<SlackMessage>> {

    @Override
    public List<SlackMessage> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<SlackMessage> add(SlackMessage value, List<SlackMessage> accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public List<SlackMessage> getResult(List<SlackMessage> accumulator) {
        return accumulator;
    }

    @Override
    public List<SlackMessage> merge(List<SlackMessage> a, List<SlackMessage> b) {
        a.addAll(b);
        return a;
    }
}
