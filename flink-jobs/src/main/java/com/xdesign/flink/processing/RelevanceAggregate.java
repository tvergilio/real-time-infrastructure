package com.xdesign.flink.processing;

import com.xdesign.flink.model.RestaurantEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * This is an implementation of Flink's AggregateFunction interface.
 * It is used to aggregate the relevance of a restaurant based on the events it receives.
 * */
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
