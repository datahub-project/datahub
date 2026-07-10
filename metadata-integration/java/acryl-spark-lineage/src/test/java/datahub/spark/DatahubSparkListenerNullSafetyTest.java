package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.JobSucceeded$;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.junit.jupiter.api.Test;

/**
 * The DataHub listener creates the underlying OpenLineage listener lazily. Several paths leave it
 * null — the listener is disabled, there is no active {@code SparkContext}/{@code SparkEnv} yet, or
 * config parsing fails. In those cases the event handlers must be no-ops rather than throwing an
 * NPE back into Spark's listener bus. This test drives the handlers in a bare JVM (no active
 * SparkContext), so lazy init never produces a delegate listener, and asserts they do not throw.
 *
 * <p>All handlers share the same {@code listenerNotReady(...)} guard, so exercising a
 * representative start/end/other set covers the fix.
 */
public class DatahubSparkListenerNullSafetyTest {

  @Test
  public void handlersAreNoOpsWhenDelegateListenerNeverInitialized() throws Exception {
    DatahubSparkListener listener =
        new DatahubSparkListener(new SparkConf().setAppName("null-safety").setMaster("local[1]"));

    // No SparkContext/SparkEnv is active in this unit test JVM, so
    // initializeContextFactoryIfNotInitialized() cannot build the delegate OpenLineageSparkListener
    // and it stays null. Before the guard these calls NPE'd on `listener.onXxx(...)`.
    assertDoesNotThrow(() -> listener.onApplicationEnd(new SparkListenerApplicationEnd(0L)));
    assertDoesNotThrow(
        () -> listener.onJobEnd(new SparkListenerJobEnd(0, 0L, JobSucceeded$.MODULE$)));
    assertDoesNotThrow(() -> listener.onOtherEvent(new SparkListenerEvent() {}));
  }
}
