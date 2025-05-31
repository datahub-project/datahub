package datahub.spark;

import datahub.spark.conf.SparkLineageConf;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryListener;

import java.net.URISyntaxException;

/**
 * Utility class to help integrate DataHub lineage capturing for Spark streaming queries.
 * This class provides methods to set up both types of streaming query listeners and log interceptors:
 * 1. MicroBatchExecution - for micro-batch processing mode
 * 2. ContinuousExecution - for continuous processing mode
 */
@Slf4j
public class SparkStreamingLineageIntegration {

    /**
     * Register all DataHub lineage components for capturing streaming query events.
     * This includes:
     * 1. The MicroBatchExecutionListener and ContinuousExecutionListener to capture streaming query events
     * 2. The log interceptors to capture logs from both execution engines
     * 
     * @param spark The SparkSession to register components with
     * @param datahubConf The DataHub lineage configuration
     * @param appName The name of the Spark application
     * @return The configured DatahubEventEmitter
     */
    public static DatahubEventEmitter setupLineageCapturing(
            SparkSession spark, 
            SparkLineageConf datahubConf,
            String appName) {
        
        try {
            log.info("Setting up DataHub lineage capturing for Spark streaming queries");
            
            // Create and configure the DataHub event emitter
            SparkContext sc = spark.sparkContext();
            SparkOpenLineageConfig openLineageConfig = 
                new SparkOpenLineageConfig(sc.getConf(), appName);
            
            DatahubEventEmitter emitter = 
                new DatahubEventEmitter(openLineageConfig, appName);
            emitter.setConfig(datahubConf);
            
            // Install the log interceptors to capture execution logs
            MicroBatchLogInterceptor.install(emitter);
            ContinuousExecutionLogInterceptor.install(emitter);
            
            // Register the streaming query listeners with Spark
            // Note: The listeners are already initialized by the emitter
            registerStreamingQueryListeners(spark, emitter);
            
            log.info("DataHub lineage capturing for streaming queries set up successfully");
            return emitter;
            
        } catch (Exception e) {
            log.error("Failed to set up DataHub lineage capturing", e);
            throw new RuntimeException("Failed to set up DataHub lineage capturing", e);
        }
    }
    
    /**
     * Register the streaming query listeners with Spark.
     */
    private static void registerStreamingQueryListeners(SparkSession spark, DatahubEventEmitter emitter) {
        // Register the MicroBatchExecutionListener
        if (emitter.microBatchListener != null) {
            spark.streams().addListener(
                new ForwardingStreamingQueryListener(emitter.microBatchListener));
            log.info("Registered MicroBatchExecutionListener with Spark");
        }
        
        // Register the ContinuousExecutionListener
        if (emitter.continuousListener != null) {
            spark.streams().addListener(
                new ForwardingStreamingQueryListener(emitter.continuousListener));
            log.info("Registered ContinuousExecutionListener with Spark");
        }
    }
    
    /**
     * Simple forwarding listener that sends events to another StreamingQueryListener.
     * This is needed because we can't register the listeners directly
     * (they're fields in the DatahubEventEmitter).
     */
    private static class ForwardingStreamingQueryListener extends StreamingQueryListener {
        private final StreamingQueryListener delegate;
        
        public ForwardingStreamingQueryListener(StreamingQueryListener delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public void onQueryStarted(QueryStartedEvent event) {
            delegate.onQueryStarted(event);
        }
        
        @Override
        public void onQueryProgress(QueryProgressEvent event) {
            delegate.onQueryProgress(event);
        }
        
        @Override
        public void onQueryTerminated(QueryTerminatedEvent event) {
            delegate.onQueryTerminated(event);
        }
    }
    
    /**
     * Example usage in a Spark application:
     * 
     * <pre>
     * public static void main(String[] args) {
     *     SparkSession spark = SparkSession.builder()
     *         .appName("StreamingApp")
     *         .getOrCreate();
     *     
     *     // Configure DataHub lineage
     *     SparkLineageConf conf = new SparkLineageConf();
     *     conf.setRestDatahubEmitterConfig(...);
     *     
     *     // Set up lineage capturing
     *     DatahubEventEmitter emitter = SparkStreamingLineageIntegration.setupLineageCapturing(
     *         spark, conf, "StreamingApp");
     *     
     *     // Define and start your streaming query (micro-batch or continuous)
     *     Dataset<Row> streamingData = spark.readStream()...
     *     
     *     // For micro-batch mode (default)
     *     StreamingQuery query = streamingData.writeStream()...
     *     
     *     // For continuous mode
     *     StreamingQuery continuousQuery = streamingData.writeStream()
     *         .trigger(Trigger.Continuous("1 second"))...
     *     
     *     // Wait for the queries to terminate
     *     query.awaitTermination();
     * }
     * </pre>
     */
} 