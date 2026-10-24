package datahub.hive.consumer.util;

/**
 * Utility class for time-related operations.
 */
public class TimeUtils {

    private TimeUtils() {
        // Private constructor to prevent instantiation
    }

    /**
     * Calculates and returns the duration in milliseconds between a start time and the current time.
     *
     * @param startTime The start time in milliseconds
     * @return The duration in milliseconds
     */
    public static long calculateDuration(long startTime) {
        return System.currentTimeMillis() - startTime;
    }
}
