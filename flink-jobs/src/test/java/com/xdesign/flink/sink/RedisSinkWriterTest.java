package com.xdesign.flink.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xdesign.flink.model.RestaurantRelevance;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class RedisSinkWriterTest {

    private RedisSink.RedisSinkWriter redisSinkWriter;
    private Jedis jedisMock;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() {
        jedisMock = mock(Jedis.class);
        redisSinkWriter = new RedisSink.RedisSinkWriter(jedisMock);
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testWrite_HappyPath() throws Exception {
        // Arrange
        var restaurantId = "1";
        var score = 4.5;
        var timestamp = 123456789L;
        var relevance = new RestaurantRelevance(restaurantId, score, timestamp);
        var json = objectMapper.writeValueAsString(relevance);

        // Act
        redisSinkWriter.write(json, mock(SinkWriter.Context.class));

        // Assert
        verify(jedisMock, times(1))
                .zincrby("restaurant_relevance", score, restaurantId);
    }

    @Test
    public void testWrite_WithIOException() {
        // Arrange
        String invalidJson = "{invalid json}";

        // Act and Assert
        try {
            redisSinkWriter.write(invalidJson, mock(SinkWriter.Context.class));
        } catch (IOException | InterruptedException e) {
            // Expected exception
        }

        // Assert
        verify(jedisMock, never())
                .zadd(anyString(), anyDouble(), anyString());
    }

    @Test
    public void testClose() throws Exception {
        // Act
        redisSinkWriter.close();

        // Assert
        verify(jedisMock, times(1)).close();
    }
}
