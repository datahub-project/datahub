package com.linkedin.metadata.dao;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * A snapshot request builder for dataset entities.
 */
public class DatasetSnapshotRequestBuilder extends BaseSnapshotRequestBuilder<DatasetSnapshot, DatasetUrn> {

  private static final String BASE_URI_TEMPLATE = "datasets/{key}/snapshot";

  public DatasetSnapshotRequestBuilder() {
    super(DatasetSnapshot.class, DatasetUrn.class, BASE_URI_TEMPLATE);
  }

  @Nonnull
  @Override
  protected Map<String, Object> pathKeys(@Nonnull DatasetUrn urn) {
    return Collections.singletonMap("key", new ComplexResourceKey<>(
        new DatasetKey().setPlatform(urn.getPlatformEntity())
            .setName(urn.getDatasetNameEntity())
            .setOrigin(urn.getOriginEntity()), new EmptyRecord()));
  }
}
