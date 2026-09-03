package com.linkedin.metadata.trace;

import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Transport-neutral failed MCP topic scan for async trace. */
public interface McpFailedTracePort {

  @Nonnull
  Map<Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>> findMessages(
      @Nonnull Map<Urn, List<String>> urnAspectPairs,
      @Nonnull String traceId,
      @Nullable Long traceTimestampMillis);

  @Nonnull
  Optional<FailedMetadataChangeProposal> read(@Nullable GenericRecord genericRecord);
}
