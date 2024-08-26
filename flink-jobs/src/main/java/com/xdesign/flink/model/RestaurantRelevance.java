package com.xdesign.flink.model;

/**
 * Simple POJO class to represent a restaurant's relevance score.
 * */
public class RestaurantRelevance {
    private String restaurantId;
    private double relevanceScore;
    private long timestamp;

    // Default constructor
    public RestaurantRelevance() {
    }

    public RestaurantRelevance(String restaurantId, double relevanceScore, long timestamp) {
        this.restaurantId = restaurantId;
        this.relevanceScore = relevanceScore;
        this.timestamp = timestamp;
    }

    public String getRestaurantId() {
        return restaurantId;
    }

    public void setRestaurantId(String restaurantId) {
        this.restaurantId = restaurantId;
    }

    public double getRelevanceScore() {
        return relevanceScore;
    }

    public void setRelevanceScore(double relevanceScore) {
        this.relevanceScore = relevanceScore;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
