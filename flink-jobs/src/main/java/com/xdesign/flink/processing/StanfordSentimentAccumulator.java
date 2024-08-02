package com.xdesign.flink.processing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xdesign.flink.model.SlackMessage;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class StanfordSentimentAccumulator {

    private final List<Integer> scores;
    private final List<String> classes;
    private long start;
    private long end;
    private double averageScore;
    private String sentiment;
    private SlackMessage mostPositiveMessage;
    private SlackMessage mostNegativeMessage;
    private int messageCount;

    public StanfordSentimentAccumulator() {
        this.scores = new ArrayList<>();
        this.classes = new ArrayList<>();
        this.messageCount = 0;
    }

    public void add(SlackMessage message, Tuple2<List<Integer>, List<String>> value) {
        scores.addAll(value.f0);
        classes.addAll(value.f1);
        messageCount++;

        int maxScore = value.f0.stream().max(Integer::compareTo).orElse(Integer.MIN_VALUE);
        int minScore = value.f0.stream().min(Integer::compareTo).orElse(Integer.MAX_VALUE);

        // Update the most positive message
        if (mostPositiveMessage == null || maxScore > getMaxScore() ||
                (maxScore == getMaxScore() && message.getMessage().length() > mostPositiveMessage.getMessage().length())) {
            mostPositiveMessage = message;
        }

        // Update the most negative message
        if (mostNegativeMessage == null || minScore < getMinScore() ||
                (minScore == getMinScore() && message.getMessage().length() < mostNegativeMessage.getMessage().length())) {
            mostNegativeMessage = message;
        }

        updateAverageScore();
        sentiment = classifySentiment(averageScore);
    }

    public void merge(StanfordSentimentAccumulator other) {
        scores.addAll(other.scores);
        classes.addAll(other.classes);
        messageCount += other.messageCount;

        updateAverageScore();

        // Retain the most positive message based on score and length of the message
        if (this.mostPositiveMessage == null ||
                (other.mostPositiveMessage != null &&
                        (other.getMaxScore() > this.getMaxScore() ||
                                (other.getMaxScore() == this.getMaxScore() && other.mostPositiveMessage.getMessage().length() > this.mostPositiveMessage.getMessage().length())))) {
            this.mostPositiveMessage = other.mostPositiveMessage;
        }

        // Retain the most negative message based on score and length of the message
        if (this.mostNegativeMessage == null ||
                (other.mostNegativeMessage != null &&
                        (other.getMinScore() < this.getMinScore() ||
                                (other.getMinScore() == this.getMinScore() && other.mostNegativeMessage.getMessage().length() < this.mostNegativeMessage.getMessage().length())))) {
            this.mostNegativeMessage = other.mostNegativeMessage;
        }

        // Set sentiment based on the new average score
        sentiment = classifySentiment(this.averageScore);
    }

    private void updateAverageScore() {
        averageScore = scores.stream().mapToInt(Integer::intValue).average().orElse(0.0);
    }

    private String classifySentiment(double averageScore) {
        if (averageScore >= 3.5) {
            return "Very positive";
        } else if (averageScore >= 2.5) {
            return "Positive";
        } else if (averageScore >= 1.5) {
            return "Neutral";
        } else if (averageScore >= 0.5) {
            return "Negative";
        } else {
            return "Very negative";
        }
    }

    private int getMaxScore() {
        return scores.stream().max(Integer::compareTo).orElse(Integer.MIN_VALUE);
    }

    private int getMinScore() {
        return scores.stream().min(Integer::compareTo).orElse(Integer.MAX_VALUE);
    }

    @Override
    public String toString() {
        return toJson();
    }

    public String toJson() {
        var mapper = new ObjectMapper();
        var jsonNode = mapper.createObjectNode();

        // Formatting dates
        var formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss").withZone(ZoneId.of("Europe/London"));
        var startFormatted = formatter.format(Instant.ofEpochMilli(start));
        var endFormatted = formatter.format(Instant.ofEpochMilli(end));

        // Construct JSON object
        jsonNode.put("start", startFormatted);
        jsonNode.put("end", endFormatted);
        jsonNode.put("overallSentiment", sentiment);
        jsonNode.put("mostPositiveMessage", mostPositiveMessage != null ? mostPositiveMessage.getMessage() : "N/A");
        jsonNode.put("mostNegativeMessage", mostNegativeMessage != null ? mostNegativeMessage.getMessage() : "N/A");
        jsonNode.put("messageCount", messageCount);
        jsonNode.put("averageScore", Double.parseDouble(String.format("%.2f", averageScore)));

        // Convert JSON object to string
        try {
            return mapper.writeValueAsString(jsonNode);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "{}";
        }
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

    public int getMessageCount() {
        return messageCount;
    }

    public double getAverageScore() {
        return averageScore;
    }

    public String getSentiment() {
        return sentiment;
    }

    public String getMostPositiveMessage() {
        return mostPositiveMessage != null ? mostPositiveMessage.getMessage() : null;
    }

    public String getMostNegativeMessage() {
        return mostNegativeMessage != null ? mostNegativeMessage.getMessage() : null;
    }
}

