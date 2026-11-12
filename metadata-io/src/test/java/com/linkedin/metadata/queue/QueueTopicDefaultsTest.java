package com.linkedin.metadata.queue;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.postgres.PgQueueResolvedTopicCatalogEntry;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import java.util.List;
import org.testng.annotations.Test;

public class QueueTopicDefaultsTest {

  private static final String BANDS_JSON =
      "[{\"range\":[0,3],\"weight\":70},{\"range\":[4,6],\"weight\":20},{\"range\":[7,9],\"weight\":10}]";

  @Test
  public void resolveForTopic_usesCatalogMatchOtherwiseFallback() {
    PgQueueResolvedTopicCatalogEntry ts =
        new PgQueueResolvedTopicCatalogEntry(
            "metadataChangeLogTimeseries",
            "MetadataChangeLog_Timeseries_v1",
            2,
            BANDS_JSON,
            7776000,
            0L,
            0L,
            false,
            1);
    PgQueueSetupOptions opts =
        new PgQueueSetupOptions(
            "queue",
            "metadata_queue",
            2,
            600,
            BANDS_JSON,
            604800,
            0L,
            0L,
            "application/avro",
            "1 day",
            4,
            false,
            3600,
            5000,
            false,
            1,
            List.of(ts));

    QueueTopicDefaults resolved =
        QueueTopicDefaults.resolveForTopic(opts, "MetadataChangeLog_Timeseries_v1");
    assertEquals(resolved.retentionMaxAgeSeconds(), 7776000);

    QueueTopicDefaults fallback = QueueTopicDefaults.resolveForTopic(opts, "OtherTopic_v1");
    assertEquals(fallback.retentionMaxAgeSeconds(), 604800);
  }
}
