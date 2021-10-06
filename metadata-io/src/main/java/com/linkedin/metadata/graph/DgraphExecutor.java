package com.linkedin.metadata.graph;

import io.dgraph.DgraphClient;
import io.dgraph.TxnConflictException;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class DgraphExecutor {
    private final DgraphClient _client;

    public DgraphExecutor(DgraphClient client) {
        this._client = client;
    }

    public <T> T execute(Function<DgraphClient, T> func) {
        int retry = 0;
        while (true) {
            try {
                return func.apply(_client);
            } catch (Exception e) {
                Throwable t = e;

                // unwrap RuntimeException and ExecutionException
                while (true) {
                    if ((t instanceof RuntimeException || t instanceof ExecutionException) && t.getCause() != null) {
                        t = t.getCause();
                        continue;
                    }
                    break;
                }

                if (t instanceof TxnConflictException
                        || t instanceof StatusRuntimeException && (
                        t.getMessage().contains("operation opIndexing is already running")
                                || t.getMessage().contains("Please retry")
                                || t.getMessage().contains("DEADLINE_EXCEEDED:")
                                || t.getMessage().contains("context deadline exceeded")
                                || t.getMessage().contains("Only leader can decide to commit or abort")
                )) {
                    try {
                        // wait 0.01s, 0.02s, 0.04s, 0.08s, ..., 10.24s
                        long time = (long) Math.pow(2, Math.min(retry, 10)) * 10;
                        synchronized (System.out) {
                            System.out.printf(System.currentTimeMillis() + ": retrying in %d ms due to %s%n", time, t.getMessage());
                        }
                        TimeUnit.MILLISECONDS.sleep(time);
                        retry++;
                    } catch (InterruptedException e2) {
                        // ignore interruption
                    }

                    continue;
                }

                // throw unexpected exceptions
                synchronized (System.out) {
                    System.out.printf(System.currentTimeMillis() + ": un-retried exception: %s%n", t.getMessage());
                }

                throw e;
            }
        }
    }
}
