package com.xdesign.flink.processing;

import com.xdesign.flink.model.RestaurantRelevance;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * This is an implementation of Flink's ProcessWindowFunction interface.
 * It is used to calculate the relevance score of a restaurant based on the aggregated view and like counts.
 */
public class RelevanceScoringFunction extends
        ProcessWindowFunction<RelevanceAccumulator, RestaurantRelevance, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<RelevanceAccumulator> elements, Collector<RestaurantRelevance> out) {
        if (elements == null || !elements.iterator().hasNext()) {
            return;
        }
        var acc = elements.iterator().next();
        if (acc.viewCount < 0 || acc.likeCount < 0) {
            throw new IllegalArgumentException("View count and like count must not be negative");
        }
        var relevanceScore = acc.viewCount * 0.1 + acc.likeCount * 1.0;  // Example scoring
        out.collect(new RestaurantRelevance(key, relevanceScore));
    }
}
