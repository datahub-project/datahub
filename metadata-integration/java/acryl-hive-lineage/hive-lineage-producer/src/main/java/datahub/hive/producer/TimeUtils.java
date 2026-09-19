package datahub.hive.producer;

/**
 * Utility class for time-related operations.
 */
public class TimeUtils {

    /**
     * Calculates and returns the duration in milliseconds between a start time and the current time.
     */
    public static long calculateDuration(long startTime) {
        return System.currentTimeMillis() - startTime;
    }
}
