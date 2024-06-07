package com.xdesign.flink;

public class RestaurantRelevance {
    private final String restaurantId;
    private final double relevanceScore;

    public RestaurantRelevance(String restaurantId, double relevanceScore) {
        this.restaurantId = restaurantId;
        this.relevanceScore = relevanceScore;
    }

    public String getRestaurantId() {
        return restaurantId;
    }

    public double getRelevanceScore() {
        return relevanceScore;
    }
}
