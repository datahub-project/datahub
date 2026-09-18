package com.datahub.graphql;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Marker body telling {@link GraphQLResponseBodyConverter} to stream this GraphQL response rather
 * than buffer it as a String (see that class for why).
 *
 * @param spec the execution result specification (response tree) to serialize
 * @param onBytesWritten invoked once, after a successful write, with the number of bytes streamed
 * @param onWriteFinished invoked once when writing finishes, with whether serialization succeeded
 */
public record GraphQLResponseBody(
    Map<String, Object> spec, LongConsumer onBytesWritten, Consumer<Boolean> onWriteFinished) {}
