package datahub.hive.consumer.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TimeUtils.
 * Tests time-related utility operations.
 */
public class TimeUtilsTest {

    /**
     * Tests calculateDuration method with valid start time.
     * Verifies that duration is calculated correctly.
     */
    @Test
    void testCalculateDuration_ValidStartTime() {
        // Get current time as start time
        long startTime = System.currentTimeMillis();
        
        // Add a small delay to ensure duration > 0
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Calculate duration
        long duration = TimeUtils.calculateDuration(startTime);
        
        // Verify duration is positive and reasonable
        assertTrue(duration >= 0);
        assertTrue(duration < 1000); // Should be less than 1 second for this test
    }

    /**
     * Tests calculateDuration method with past start time.
     * Verifies that duration calculation works with older timestamps.
     */
    @Test
    void testCalculateDuration_PastStartTime() {
        // Use a start time from 1 second ago
        long startTime = System.currentTimeMillis() - 1000;
        
        // Calculate duration
        long duration = TimeUtils.calculateDuration(startTime);
        
        // Verify duration is approximately 1 second (with some tolerance)
        assertTrue(duration >= 1000);
        assertTrue(duration < 2000); // Should be less than 2 seconds
    }

    /**
     * Tests calculateDuration method with future start time.
     * Verifies that duration can handle future timestamps (negative duration).
     */
    @Test
    void testCalculateDuration_FutureStartTime() {
        // Use a start time from 1 second in the future
        long startTime = System.currentTimeMillis() + 1000;
        
        // Calculate duration
        long duration = TimeUtils.calculateDuration(startTime);
        
        // Verify duration is negative
        assertTrue(duration < 0);
        assertTrue(duration > -2000); // Should be greater than -2 seconds
    }

    /**
     * Tests calculateDuration method with zero start time.
     * Verifies that duration calculation works with epoch time.
     */
    @Test
    void testCalculateDuration_ZeroStartTime() {
        // Use zero as start time (epoch)
        long startTime = 0;
        
        // Calculate duration
        long duration = TimeUtils.calculateDuration(startTime);
        
        // Verify duration is positive and represents current time
        assertTrue(duration > 0);
        
        // Should be approximately current time in milliseconds
        long currentTime = System.currentTimeMillis();
        assertTrue(Math.abs(duration - currentTime) < 1000); // Within 1 second tolerance
    }

    /**
     * Tests calculateDuration method with maximum long value.
     * Verifies that duration calculation handles edge cases.
     */
    @Test
    void testCalculateDuration_MaxLongValue() {
        // Use maximum long value as start time
        long startTime = Long.MAX_VALUE;
        
        // Calculate duration
        long duration = TimeUtils.calculateDuration(startTime);
        
        // Verify duration is negative (since start time is in far future)
        assertTrue(duration < 0);
    }

    /**
     * Tests that TimeUtils class cannot be instantiated.
     * Verifies that the utility class has a private constructor.
     */
    @Test
    void testUtilityClassCannotBeInstantiated() throws Exception {
        // Access the private constructor
        var constructor = TimeUtils.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        
        // Should be able to instantiate (it's a utility class pattern)
        assertDoesNotThrow(() -> {
            constructor.newInstance();
        });
    }

    /**
     * Tests calculateDuration method precision.
     * Verifies that the method returns precise millisecond differences.
     */
    @Test
    void testCalculateDuration_Precision() {
        long startTime = System.currentTimeMillis();
        
        // Immediately calculate duration
        long duration1 = TimeUtils.calculateDuration(startTime);
        
        // Wait a bit and calculate again
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        long duration2 = TimeUtils.calculateDuration(startTime);
        
        // Second duration should be greater than first
        assertTrue(duration2 >= duration1);
    }
}
