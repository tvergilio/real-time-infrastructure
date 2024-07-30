package com.xdesign.flink.processing;

import com.xdesign.flink.model.SlackMessage;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.List;

public interface SentimentProcessingFunction extends MapFunction<SlackMessage, Tuple2<SlackMessage, Tuple2<List<Integer>, List<String>>>> {
    void open(Configuration configuration);
    Tuple2<SlackMessage, Tuple2<List<Integer>, List<String>>> map(SlackMessage slackMessage);
}
