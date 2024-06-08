package com.xdesign.flink;

import org.apache.flink.api.common.functions.AggregateFunction;

public class RelevanceAggregate
        implements AggregateFunction<RestaurantEvent, RelevanceAccumulator, RelevanceAccumulator> {
    @Override
    public RelevanceAccumulator createAccumulator() {
        return new RelevanceAccumulator();
    }

    @Override
    public RelevanceAccumulator add(RestaurantEvent event, RelevanceAccumulator accumulator) {
        if (event.getEventType().equals("view")) {
            accumulator.viewCount += 1;
        } else if (event.getEventType().equals("like")) {
            accumulator.likeCount += 1;
        }
        return accumulator;
    }

    @Override
    public RelevanceAccumulator getResult(RelevanceAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public RelevanceAccumulator merge(RelevanceAccumulator a, RelevanceAccumulator b) {
        a.viewCount += b.viewCount;
        a.likeCount += b.likeCount;
        return a;
    }
}
