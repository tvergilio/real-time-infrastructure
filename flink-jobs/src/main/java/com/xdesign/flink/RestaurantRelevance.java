package com.xdesign.flink;

public class RestaurantRelevance {
    private String restaurantId;
    private double relevanceScore;

    // Default constructor
    public RestaurantRelevance() {}

    // Parameterized constructor
    public RestaurantRelevance(String restaurantId, double relevanceScore) {
        this.restaurantId = restaurantId;
        this.relevanceScore = relevanceScore;
    }

    // Getter methods
    public String getRestaurantId() {
        return restaurantId;
    }

    public double getRelevanceScore() {
        return relevanceScore;
    }

    // Setter methods
    public void setRestaurantId(String restaurantId) {
        this.restaurantId = restaurantId;
    }

    public void setRelevanceScore(double relevanceScore) {
        this.relevanceScore = relevanceScore;
    }
}
