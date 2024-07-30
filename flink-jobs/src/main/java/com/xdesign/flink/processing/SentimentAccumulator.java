package com.xdesign.flink.processing;

import com.xdesign.flink.model.SlackMessage;
import org.apache.flink.api.java.tuple.Tuple2;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SentimentAccumulator {
    private final List<Integer> scores = new ArrayList<>();
    private SlackMessage mostPositiveMessage;
    private SlackMessage mostNegativeMessage;
    private int highestScore = Integer.MIN_VALUE;
    private int lowestScore = Integer.MAX_VALUE;
    private long start;
    private long end;
    private int count = 0;

    private static final DecimalFormat df = new DecimalFormat("0.00");

    public void add(SlackMessage message, Tuple2<List<Integer>, List<String>> sentiment, long start, long end) {
        scores.addAll(sentiment.f0);

        this.start = start;
        this.end = end;
        this.count++;

        for (int score : sentiment.f0) {
            if (score > highestScore) {
                highestScore = score;
                mostPositiveMessage = message;
            }
            if (score < lowestScore) {
                lowestScore = score;
                mostNegativeMessage = message;
            }
        }
    }

    public void merge(SentimentAccumulator other) {
        scores.addAll(other.scores);
        count += other.count;

        if (other.highestScore > highestScore) {
            highestScore = other.highestScore;
            mostPositiveMessage = other.mostPositiveMessage;
        }

        if (other.lowestScore < lowestScore) {
            lowestScore = other.lowestScore;
            mostNegativeMessage = other.mostNegativeMessage;
        }
    }

    public double getAverageScore() {
        return scores.stream().mapToInt(Integer::intValue).average().orElse(0.0);
    }

    public String getAverageClass() {
        double averageScore = getAverageScore();
        if (averageScore >= 2.5) {
            return "Positive";
        } else if (averageScore >= 1.5) {
            return "Neutral";
        } else {
            return "Negative";
        }
    }

    public SlackMessage getMostPositiveMessage() {
        return mostPositiveMessage;
    }

    public SlackMessage getMostNegativeMessage() {
        return mostNegativeMessage;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public String formatTimestamp(long timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        return sdf.format(new Date(timestamp));
    }

    @Override
    public String toString() {
        return "SentimentAccumulator{" +
                "start=" + formatTimestamp(start) +
                ", end=" + formatTimestamp(end) +
                ", averageScore=" + df.format(getAverageScore()) +
                ", averageClass=" + getAverageClass() +
                ", messageCount=" + count +
                ", mostPositiveMessage='" + (mostPositiveMessage != null ? mostPositiveMessage.getMessage() : "N/A") + '\'' +
                ", mostNegativeMessage='" + (mostNegativeMessage != null ? mostNegativeMessage.getMessage() : "N/A") + '\'' +
                '}';
    }

    public int getCount() {
        return count;
    }
}
