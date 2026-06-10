package com.linkedin.gms.factory.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import io.datahubproject.event.models.v1.ExternalEvents;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.testng.annotations.Test;

public class PgQueueExternalEventsPollHandlerTest {

  @Test
  public void poll_returnsEmptyWhenNoMessages() throws Exception {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    PostgresSqlSetupProperties postgresProperties = mock(PostgresSqlSetupProperties.class);
    when(postgresProperties.buildPgQueueOptions(any())).thenReturn(null);

    when(store.fetchTopic("events"))
        .thenReturn(Optional.of(new QueueTopicMetadata(1L, 1, Optional.empty())));
    when(store.partitionNextExclusiveSeqs(1L, 1)).thenReturn(Collections.singletonMap(0, 0L));
    when(store.peekTopicLog(eq(1L), any(), anyInt())).thenReturn(List.of());

    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);

    PgQueueExternalEventsPollHandler handler =
        new PgQueueExternalEventsPollHandler(
            store, postgresProperties, deserializer, new ObjectMapper(), null);

    ExternalEvents events = handler.poll("events", null, 10, 50L, null);

    assertEquals(events.getEvents().size(), 0);
    assertEquals(events.getCount(), Long.valueOf(0));
  }

  @Test
  public void poll_ensuresTopicWhenMissing() throws Exception {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    PostgresSqlSetupProperties postgresProperties = mock(PostgresSqlSetupProperties.class);
    PgQueueSetupOptions options = mock(PgQueueSetupOptions.class);
    when(postgresProperties.buildPgQueueOptions(any())).thenReturn(options);
    when(options.getResolvedTopicCatalog()).thenReturn(List.of());

    when(store.fetchTopic("events")).thenReturn(Optional.empty());
    when(store.ensureTopic(eq("events"), any())).thenReturn(2L);
    when(store.fetchTopic("events"))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(new QueueTopicMetadata(2L, 1, Optional.empty())));
    when(store.partitionNextExclusiveSeqs(2L, 1)).thenReturn(Collections.singletonMap(0, 0L));
    when(store.peekTopicLog(eq(2L), any(), anyInt())).thenReturn(List.of());

    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);

    PgQueueExternalEventsPollHandler handler =
        new PgQueueExternalEventsPollHandler(
            store, postgresProperties, deserializer, new ObjectMapper(), null);

    ExternalEvents events = handler.poll("events", null, 5, 10L, null);

    assertEquals(events.getCount(), Long.valueOf(0));
  }

  @Test
  public void poll_usesLookbackWhenNoOffsetId() throws Exception {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    PostgresSqlSetupProperties postgresProperties = mock(PostgresSqlSetupProperties.class);
    when(postgresProperties.buildPgQueueOptions(any())).thenReturn(null);

    when(store.fetchTopic("events"))
        .thenReturn(Optional.of(new QueueTopicMetadata(3L, 2, Optional.empty())));
    when(store.partitionNextExclusiveSeqs(3L, 2)).thenReturn(Map.of(0, 100L, 1, 50L));
    when(store.minEnqueueSeqAtOrAfter(eq(3L), eq(0), any(Instant.class)))
        .thenReturn(OptionalLong.of(10L));
    when(store.minEnqueueSeqAtOrAfter(eq(3L), eq(1), any(Instant.class)))
        .thenReturn(OptionalLong.empty());
    when(store.minEnqueueSeq(eq(3L), eq(1))).thenReturn(OptionalLong.of(5L));
    when(store.peekTopicLog(eq(3L), any(), anyInt())).thenReturn(List.of());

    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);

    PgQueueExternalEventsPollHandler handler =
        new PgQueueExternalEventsPollHandler(
            store, postgresProperties, deserializer, new ObjectMapper(), null);

    ExternalEvents events = handler.poll("events", null, 5, 10L, 7);

    assertEquals(events.getCount(), Long.valueOf(0));
    verify(store).minEnqueueSeqAtOrAfter(eq(3L), eq(0), any(Instant.class));
    verify(store).minEnqueueSeqAtOrAfter(eq(3L), eq(1), any(Instant.class));
    verify(store).minEnqueueSeq(3L, 1);
  }
}
