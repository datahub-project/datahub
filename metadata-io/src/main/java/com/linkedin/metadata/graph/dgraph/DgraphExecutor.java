package com.linkedin.metadata.graph.dgraph;

import io.dgraph.DgraphClient;
import io.dgraph.TxnConflictException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class DgraphExecutor {

    // requests are retried with an exponential randomized backoff
    // wait 0.01s, 0.02s, 0.04s, 0.08s, ..., 10s, all ±50%
    private static final Duration INITIAL_DURATION = Duration.ofMillis(10);
    private static final Duration MAX_DURATION = Duration.ofSeconds(10);
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private static final double RANDOMIZATION_FACTOR = 0.5;

    private final DgraphClient _client;
    private final Retry _retry;

    public DgraphExecutor(DgraphClient client, int maxAttempts) {
        this._client = client;

        RetryConfig config = RetryConfig.custom()
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(INITIAL_DURATION, BACKOFF_MULTIPLIER, RANDOMIZATION_FACTOR, MAX_DURATION))
                .retryOnException(DgraphExecutor::isRetryableException)
                .failAfterMaxAttempts(true)
                .maxAttempts(maxAttempts)
                .build();
        this._retry = Retry.of("DgraphExecutor", config);
    }

    /**
     * Executes the given DgraphClient call and retries retry-able exceptions.
     * Subsequent executions will experience an exponential randomized backoff.
     *
     * @param func call on the provided DgraphClient
     * @param <T> return type of the function
     * @return return value of the function
     * @throws io.github.resilience4j.retry.MaxRetriesExceeded if max attempts exceeded
     */
    public <T> T executeFunction(Function<DgraphClient, T> func) {
        return Retry.decorateFunction(this._retry, func).apply(_client);
    }

    /**
     * Executes the given DgraphClient call and retries retry-able exceptions.
     * Subsequent executions will experience an exponential randomized backoff.
     *
     * @param func call on the provided DgraphClient
     * @throws io.github.resilience4j.retry.MaxRetriesExceeded if max attempts exceeded
     */
    public void executeConsumer(Consumer<DgraphClient> func) {
        this._retry.executeSupplier(() -> {
            func.accept(_client);
            return null;
        });
    }

    /**
     * Defines which DgraphClient exceptions are being retried.
     *
     * @param t exception from DgraphClient
     * @return true if this exception can be retried
     */
    private static boolean isRetryableException(Throwable t) {
        // unwrap RuntimeException and ExecutionException
        while (true) {
            if ((t instanceof RuntimeException || t instanceof ExecutionException) && t.getCause() != null) {
                t = t.getCause();
                continue;
            }
            break;
        }

        // retry-able exceptions
        if (t instanceof TxnConflictException
                || t instanceof StatusRuntimeException && (
                t.getMessage().contains("operation opIndexing is already running")
                        || t.getMessage().contains("Please retry")
                        || t.getMessage().contains("DEADLINE_EXCEEDED:")
                        || t.getMessage().contains("context deadline exceeded")
                        || t.getMessage().contains("Only leader can decide to commit or abort")
        )) {
            log.debug("retrying request due to {}", t.getMessage());
            return true;
        }
        return false;
    }
}
