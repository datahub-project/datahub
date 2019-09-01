package com.linkedin.metadata.dao;

import com.linkedin.common.urn.DatasetGroupUrn;
import com.linkedin.datasetGroup.DatasetGroupKey;
import com.linkedin.metadata.snapshot.DatasetGroupSnapshot;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * A snapshot request builder for registered schema entities.
 */
public class DatasetGroupSnapshotRequestBuilder
    extends BaseSnapshotRequestBuilder<DatasetGroupSnapshot, DatasetGroupUrn> {

  private static final String BASE_URI_TEMPLATE = "datasetGroups/{key}/snapshot";

  public DatasetGroupSnapshotRequestBuilder() {
    super(DatasetGroupSnapshot.class, DatasetGroupUrn.class, BASE_URI_TEMPLATE);
  }

  @Nonnull
  @Override
  protected Map<String, Object> pathKeys(@Nonnull DatasetGroupUrn urn) {
    return Collections.singletonMap("key", new ComplexResourceKey<>(
        new DatasetGroupKey().setNamespace(urn.getNamespaceEntity()).setName(urn.getNameEntity()), new EmptyRecord()));
  }
}
