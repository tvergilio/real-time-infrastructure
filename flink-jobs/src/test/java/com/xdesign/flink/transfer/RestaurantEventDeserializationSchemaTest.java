package com.xdesign.flink.transfer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RestaurantEventDeserializationSchemaTest {

    @Test
    public void deserialize_whenValidJson_returnsRestaurantEvent() throws Exception {
        // Arrange
        var schema = new RestaurantEventDeserializationSchema();
        var json = "{\"restaurantId\":\"1\",\"eventType\":\"view\",\"timestamp\":\"2024-06-07T15:55:00Z\"}".getBytes(StandardCharsets.UTF_8);

        // Act
        var event = schema.deserialize(json);

        // Assert
        assertEquals("1", event.getRestaurantId());
        assertEquals("view", event.getEventType());
        assertEquals("2024-06-07T15:55:00Z", event.getTimestamp());
    }

    @Test
    public void deserialize_whenInvalidJson_returnsNull() throws IOException {
        // Arrange
        var schema = new RestaurantEventDeserializationSchema();
        var json = "invalid json".getBytes(StandardCharsets.UTF_8);

        // Act
        var event = schema.deserialize(json);

        // Assert
        assertNull(event);
    }

    @Test
    public void deserialize_whenNullJson_throwsInvalidMessageException() {
        // Arrange
        var schema = new RestaurantEventDeserializationSchema();

        // Act & Assert
        assertThrows(InvalidMessageException.class, () -> schema.deserialize(null));
    }
}
