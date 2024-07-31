package com.xdesign.flink.processing;

import com.xdesign.flink.model.SlackMessage;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.List;

public class GPT4WindowProcessingFunction extends ProcessAllWindowFunction<List<SlackMessage>, GPT4SentimentAccumulator, TimeWindow> {

    private final GPT4ProcessingFunction gpt4ProcessingFunction;

    public GPT4WindowProcessingFunction(GPT4ProcessingFunction gpt4ProcessingFunction) {
        this.gpt4ProcessingFunction = gpt4ProcessingFunction;
    }

    @Override
    public void process(Context context, Iterable<List<SlackMessage>> elements, Collector<GPT4SentimentAccumulator> out) throws Exception {
        var messages = elements.iterator().next();
        var windowStart = context.window().getStart();
        var windowEnd = context.window().getEnd();
        var accumulator = gpt4ProcessingFunction.map(messages, windowStart, windowEnd);
        accumulator.setStart(context.window().getStart());
        accumulator.setEnd(context.window().getEnd());
        out.collect(accumulator);
    }
}
