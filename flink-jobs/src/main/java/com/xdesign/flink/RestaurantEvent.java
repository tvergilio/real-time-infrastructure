package com.xdesign.flink;

public class RestaurantEvent {
    private final String restaurantId;
    private final String eventType;
    private final String timestamp;

    public RestaurantEvent(String restaurantId, String eventType, String timestamp) {
        this.restaurantId = restaurantId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    public String getRestaurantId() {
        return restaurantId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
