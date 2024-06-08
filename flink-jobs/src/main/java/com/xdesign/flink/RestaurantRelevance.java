package com.xdesign.flink;

public class RestaurantRelevance {
    private String restaurantId;
    private double relevanceScore;

    // Default constructor
    public RestaurantRelevance() {
    }

    public RestaurantRelevance(String restaurantId, double relevanceScore) {
        this.restaurantId = restaurantId;
        this.relevanceScore = relevanceScore;
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
}
