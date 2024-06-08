package com.xdesign.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class RestaurantEventDeserializationSchema implements DeserializationSchema<RestaurantEvent> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public RestaurantEvent deserialize(byte[] message) throws IOException {
        try {
            return mapper.readValue(message, RestaurantEvent.class);
        } catch (Exception e) {
            // Log the exception and return null
            System.err.println("Failed to deserialize message: " + e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(RestaurantEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RestaurantEvent> getProducedType() {
        return TypeExtractor.getForClass(RestaurantEvent.class);
    }
}
