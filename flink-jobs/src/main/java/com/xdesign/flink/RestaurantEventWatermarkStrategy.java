package com.xdesign.flink;

import org.apache.flink.api.common.eventtime.*;

/**
 * Watermark strategy for RestaurantEvent.
 * Currently, sets the timestamp to record arrival time.
 * When real records are used, this can be changed to the actual event time.
 */
public class RestaurantEventWatermarkStrategy implements WatermarkStrategy<RestaurantEvent> {

    @Override
    public WatermarkGenerator<RestaurantEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new RestaurantEventWatermarkGenerator();
    }

    private static class RestaurantEventWatermarkGenerator implements WatermarkGenerator<RestaurantEvent>, SerializableTimestampAssigner<RestaurantEvent> {

        @Override
        public void onEvent(RestaurantEvent event, long eventTimestamp, WatermarkOutput output) {
            output.emitWatermark(new Watermark(System.currentTimeMillis() - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(System.currentTimeMillis() - 1));
        }

        @Override
        public long extractTimestamp(RestaurantEvent element, long recordTimestamp) {
            return System.currentTimeMillis();
        }
    }
}
