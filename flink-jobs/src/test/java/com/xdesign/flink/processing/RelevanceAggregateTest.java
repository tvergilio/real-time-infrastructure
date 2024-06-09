package com.xdesign.flink.processing;

import com.xdesign.flink.model.RestaurantEvent;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RelevanceAggregateTest {
    @Test
    public void testAdd_whenViewAndLikeEvents_incrementsCorrectCounts() {
        // Setup
        var relevanceAggregate = new RelevanceAggregate();
        var accumulator = new RelevanceAccumulator();
        var viewEvent = new RestaurantEvent("1", "view", "2024-06-07T15:55:00Z");
        var likeEvent = new RestaurantEvent("1", "like", "2024-06-07T15:55:00Z");

        // Act
        accumulator = relevanceAggregate.add(viewEvent, accumulator);

        // Assert
        assertEquals(1, accumulator.viewCount);
        assertEquals(0, accumulator.likeCount);

        // Act
        accumulator = relevanceAggregate.add(likeEvent, accumulator);

        // Assert
        assertEquals(1, accumulator.viewCount);
        assertEquals(1, accumulator.likeCount);
    }

    @Test
    public void testAdd_whenUnknownEventType_doesNotChangeCounts() {
        // Arrange
        var relevanceAggregate = new RelevanceAggregate();
        var accumulator = new RelevanceAccumulator();
        var unknownEvent = new RestaurantEvent("1", "unknown", "2024-06-07T15:55:00Z");

        // Act
        accumulator = relevanceAggregate.add(unknownEvent, accumulator);

        // Assert
        assertEquals(0, accumulator.viewCount);
        assertEquals(0, accumulator.likeCount);
    }

    @Test
    public void testAdd_whenNullEvent_throwsNullPointerException() {
        // Arrange
        var relevanceAggregate = new RelevanceAggregate();
        var accumulator = new RelevanceAccumulator();

        // Act and Assert
        assertThrows(NullPointerException.class, () -> relevanceAggregate.add(null, accumulator));
    }

    @Test
    public void testMerge_whenNullAccumulator_throwsNullPointerException() {
        // Arrange
        var relevanceAggregate = new RelevanceAggregate();
        var accumulator = new RelevanceAccumulator();

        // Act and Assert
        assertThrows(NullPointerException.class, () -> relevanceAggregate.merge(accumulator, null));
    }
}
