package com.xdesign.flink.processing;

public class GPT4SentimentAccumulator {

    private String result;
    private long start;
    private long end;

    public GPT4SentimentAccumulator(String result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return result;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public String getResult() {
        return result;
    }
}
