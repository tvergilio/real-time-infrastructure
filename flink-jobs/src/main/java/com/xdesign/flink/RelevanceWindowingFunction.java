package com.xdesign.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class RelevanceWindowingFunction extends
		ProcessWindowFunction<RelevanceAccumulator, RestaurantRelevance, String, TimeWindow> {
	@Override
	public void process(String key, Context context, Iterable<RelevanceAccumulator> elements, Collector<RestaurantRelevance> out) {
		RelevanceAccumulator acc = elements.iterator().next();
		double relevanceScore = acc.viewCount * 0.1 + acc.likeCount * 1.0;  // Example scoring
		out.collect(new RestaurantRelevance(key, relevanceScore));
	}
}
