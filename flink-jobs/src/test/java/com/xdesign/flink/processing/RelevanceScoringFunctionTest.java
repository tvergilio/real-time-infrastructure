package com.xdesign.flink.processing;

import com.xdesign.flink.model.RestaurantRelevance;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RelevanceScoringFunctionTest {

    @Test
    void process_whenCalled_calculatesRelevanceScore() throws Exception {
        // Arrange
        var function = new RelevanceScoringFunction();
        var mockContext = mock(ProcessWindowFunction.Context.class);
        var mockWindow = mock(TimeWindow.class);
        when(mockContext.window()).thenReturn(mockWindow);
        when(mockWindow.getEnd()).thenReturn(1620000000000L);
        var mockCollector = mock(Collector.class);
        var accumulator = new RelevanceAccumulator();
        accumulator.viewCount = 10;
        accumulator.likeCount = 5;

        // Act
        function.process("restaurantId", mockContext, java.util.Collections.singleton(accumulator), mockCollector);

        // Verify
        var argumentCaptor = ArgumentCaptor.forClass(RestaurantRelevance.class);
        verify(mockCollector, times(1)).collect(argumentCaptor.capture());

        // Assert
        var capturedRelevance = argumentCaptor.getValue();
        assertEquals("restaurantId", capturedRelevance.getRestaurantId());
        assertEquals(10 * 0.1 + 5 * 1.0, capturedRelevance.getRelevanceScore());
        assertEquals(1620000000000L, capturedRelevance.getTimestamp());
    }

    @Test
    void process_whenElementsIsEmpty_doesNotThrowException() {
        // Arrange
        var function = new RelevanceScoringFunction();
        var mockContext = mock(ProcessWindowFunction.Context.class);
        var mockCollector = mock(Collector.class);

        // Act and Assert
        assertDoesNotThrow(() -> function.process("restaurantId", mockContext, Collections.emptyList(), mockCollector));
    }

    @Test
    void process_whenMultipleAccumulators_takesFirstOne() {
        // Arrange
        var function = new RelevanceScoringFunction();
        var mockContext = mock(ProcessWindowFunction.Context.class);
        var mockWindow = mock(TimeWindow.class);
        when(mockContext.window()).thenReturn(mockWindow);
        when(mockWindow.getEnd()).thenReturn(1620000000000L);
        var mockCollector = mock(Collector.class);
        var accumulator1 = new RelevanceAccumulator();
        accumulator1.viewCount = 10;
        accumulator1.likeCount = 5;
        var accumulator2 = new RelevanceAccumulator();
        accumulator2.viewCount = 20;
        accumulator2.likeCount = 10;

        // Act
        function.process("restaurantId", mockContext, Arrays.asList(accumulator1, accumulator2), mockCollector);

        // Verify
        var argumentCaptor = ArgumentCaptor.forClass(RestaurantRelevance.class);
        verify(mockCollector, times(1)).collect(argumentCaptor.capture());

        // Assert
        var capturedRelevance = argumentCaptor.getValue();
        assertEquals("restaurantId", capturedRelevance.getRestaurantId());
        assertEquals(10 * 0.1 + 5 * 1.0, capturedRelevance.getRelevanceScore());
        assertEquals(1620000000000L, capturedRelevance.getTimestamp());
    }

    @Test
    void process_whenCountsAreZero_calculatesRelevanceScoreAsZero() {
        // Arrange
        var function = new RelevanceScoringFunction();
        var mockContext = mock(ProcessWindowFunction.Context.class);
        var mockWindow = mock(TimeWindow.class);
        var mockCollector = mock(Collector.class);
        when(mockContext.window()).thenReturn(mockWindow);
        when(mockWindow.getEnd()).thenReturn(1620000000000L);

        var accumulator = new RelevanceAccumulator();
        accumulator.viewCount = 0;
        accumulator.likeCount = 0;

        // Act
        function.process("restaurantId", mockContext, Collections.singleton(accumulator), mockCollector);

        // Verify
        var argumentCaptor = ArgumentCaptor.forClass(RestaurantRelevance.class);
        verify(mockCollector, times(1)).collect(argumentCaptor.capture());

        // Assert
        var capturedRelevance = argumentCaptor.getValue();
        assertEquals("restaurantId", capturedRelevance.getRestaurantId());
        assertEquals(0, capturedRelevance.getRelevanceScore());
        assertEquals(1620000000000L, capturedRelevance.getTimestamp());
    }

    @Test
    void process_whenCountsAreNegative_throwsException() {
        // Arrange
        var function = new RelevanceScoringFunction();
        var mockContext = mock(ProcessWindowFunction.Context.class);
        var mockCollector = mock(Collector.class);
        var accumulator = new RelevanceAccumulator();
        accumulator.viewCount = -10;
        accumulator.likeCount = -5;

        // Act and Assert
        assertThrows(IllegalArgumentException.class, () -> function.process("restaurantId", mockContext, Collections.singleton(accumulator), mockCollector));
    }
}
