package com.xdesign.flink.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xdesign.flink.model.RestaurantRelevance;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class RedisSink implements Sink<String> {

    private final String redisHost;
    private final int redisPort;

    public RedisSink(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        return new RedisSinkWriter(redisHost, redisPort);
    }

    public static class RedisSinkWriter implements SinkWriter<String> {
        private transient Jedis jedis;
        private final String redisHost;
        private final int redisPort;

        public RedisSinkWriter(String redisHost, int redisPort) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
            this.jedis = new Jedis(redisHost, redisPort);
        }
        // Constructor for testing
        public RedisSinkWriter(Jedis jedis) {
            this.redisHost = null;
            this.redisPort = 0;
            this.jedis = jedis;
        }

        @Override
        public void write(String element, Context context) throws IOException, InterruptedException {
            try {
                var mapper = new ObjectMapper();
                var relevance = mapper.readValue(element, RestaurantRelevance.class);
                var restaurantId = relevance.getRestaurantId();
                var score = relevance.getRelevanceScore();
                var key = "restaurant_relevance";
                jedis.zincrby(key, score, restaurantId);
                jedis.expireAt(key, getExpiryTime(relevance.getTimestamp()));
            } catch (Exception e) {
                throw new IOException("Failed to write to Redis", e);
            }
        }

        private long getExpiryTime(long timestamp) {
            long expirationTime = 30 + 2; // 30 seconds window + 2 seconds allowed lateness
            return timestamp + expirationTime * 1000;
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            // No-op
        }

        @Override
        public void close() throws Exception {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
