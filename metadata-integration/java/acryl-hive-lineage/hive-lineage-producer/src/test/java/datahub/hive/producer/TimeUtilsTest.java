package datahub.hive.producer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TimeUtils class.
 */
class TimeUtilsTest {

    @Test
    void testCalculateDuration_withValidStartTime() {
        // Given
        long startTime = System.currentTimeMillis() - 1000; // 1 second ago
        
        // When
        long duration = TimeUtils.calculateDuration(startTime);
        
        // Then
        assertTrue(duration >= 1000, "Duration should be at least 1000ms");
        assertTrue(duration < 2000, "Duration should be less than 2000ms");
    }

    @Test
    void testCalculateDuration_withCurrentTime() {
        // Given
        long startTime = System.currentTimeMillis();
        
        // When
        long duration = TimeUtils.calculateDuration(startTime);
        
        // Then
        assertTrue(duration >= 0, "Duration should be non-negative");
        assertTrue(duration < 100, "Duration should be very small for current time");
    }

    @Test
    void testCalculateDuration_withMultipleInvocations() throws InterruptedException {
        // Given
        long startTime = System.currentTimeMillis();
        Thread.sleep(100); // Sleep for 100ms
        
        // When
        long duration1 = TimeUtils.calculateDuration(startTime);
        Thread.sleep(100); // Sleep for another 100ms
        long duration2 = TimeUtils.calculateDuration(startTime);
        
        // Then
        assertTrue(duration2 > duration1, "Second duration should be greater than first");
        assertTrue(duration2 - duration1 >= 100, "Difference should be at least 100ms");
    }

    @Test
    void testCalculateDuration_withOldStartTime() {
        // Given
        long startTime = System.currentTimeMillis() - 5000; // 5 seconds ago
        
        // When
        long duration = TimeUtils.calculateDuration(startTime);
        
        // Then
        assertTrue(duration >= 5000, "Duration should be at least 5000ms");
        assertTrue(duration < 6000, "Duration should be less than 6000ms");
    }

    @Test
    void testCalculateDuration_precision() {
        // Given
        long startTime = System.currentTimeMillis();
        
        // When - call immediately
        long duration = TimeUtils.calculateDuration(startTime);
        
        // Then - should return a precise value
        assertNotNull(duration);
        assertTrue(duration >= 0, "Duration should be non-negative");
    }
}
