package com.xdesign.flink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaSerialisationSchema implements SerializationSchema<RestaurantRelevance>, DeserializationSchema<RestaurantRelevance> {
	private final ObjectMapper mapper = new ObjectMapper();

	@Override
	public byte[] serialize(RestaurantRelevance element) {
		try {
			return mapper.writeValueAsBytes(element);
		} catch (Exception e) {
			throw new RuntimeException("Failed to serialize element", e);
		}
	}

	@Override
	public RestaurantRelevance deserialize(byte[] message) {
		try {
			return mapper.readValue(message, RestaurantRelevance.class);
		} catch (Exception e) {
			throw new RuntimeException("Failed to deserialize message", e);
		}
	}

	@Override
	public boolean isEndOfStream(RestaurantRelevance nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RestaurantRelevance> getProducedType() {
		return TypeInformation.of(RestaurantRelevance.class);
	}
}
