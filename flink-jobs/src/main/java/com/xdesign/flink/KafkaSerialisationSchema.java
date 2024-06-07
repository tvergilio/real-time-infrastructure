package com.xdesign.flink;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class KafkaSerialisationSchema implements KafkaRecordSerializationSchema<RestaurantRelevance> {
	private final ObjectMapper mapper = new ObjectMapper();

	@Nullable
	@Override
	public ProducerRecord<byte[], byte[]> serialize(RestaurantRelevance restaurantRelevance, KafkaSinkContext kafkaSinkContext, Long aLong) {
		try {
			byte[] key = restaurantRelevance.getRestaurantId().getBytes();
			byte[] value = mapper.writeValueAsBytes(restaurantRelevance);
			return new ProducerRecord<>("restaurant_relevance", key, value);
		} catch (Exception e) {
			throw new RuntimeException("Failed to serialize element", e);
		}
	}
}
