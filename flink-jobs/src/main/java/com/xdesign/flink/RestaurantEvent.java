package com.xdesign.flink;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RestaurantEvent {
    private final String restaurantId;
    private final String eventType;
    private final String timestamp;

    @JsonCreator
    public RestaurantEvent(
            @JsonProperty("restaurantId") String restaurantId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("timestamp") String timestamp
    ) {
        this.restaurantId = restaurantId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    // The following constructors are used for deserialization
    public RestaurantEvent(String restaurantId) {
        this.restaurantId = restaurantId;
        this.eventType = null;
        this.timestamp = null;
    }

    public RestaurantEvent() {
        this.restaurantId = null;
        this.eventType = null;
        this.timestamp = null;
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
