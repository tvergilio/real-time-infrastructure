package com.xdesign.flink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaSerialisationSchema implements SerializationSchema<RestaurantRelevance> {
	private static final long serialVersionUID = 1L;
	private transient ObjectMapper mapper;

	@Override
	public void open(InitializationContext context) throws Exception {
		mapper = new ObjectMapper();
	}

	@Override
	public byte[] serialize(RestaurantRelevance element) {
		try {
			return mapper.writeValueAsBytes(element);
		} catch (Exception e) {
			throw new RuntimeException("Failed to serialize element", e);
		}
	}
}
