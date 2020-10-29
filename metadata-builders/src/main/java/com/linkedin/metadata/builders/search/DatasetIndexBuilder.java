package com.linkedin.metadata.builders.search;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.search.DatasetDocument;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.schema.SchemaMetadata;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class DatasetIndexBuilder extends BaseIndexBuilder<DatasetDocument> {
  public DatasetIndexBuilder() {
    super(Collections.singletonList(DatasetSnapshot.class), DatasetDocument.class);
  }

  @Nonnull
  private static String buildBrowsePath(@Nonnull DatasetUrn urn) {
    return ("/" + urn.getOriginEntity() + "/"  + urn.getPlatformEntity().getPlatformNameEntity() + "/" + urn.getDatasetNameEntity())
        .replace('.', '/').toLowerCase();
  }

  /**
   * Given dataset urn, this returns a {@link DatasetDocument} model that has urn, dataset name, platform and origin fields set
   *
   * @param urn {@link DatasetUrn} that needs to be set
   * @return {@link DatasetDocument} model with relevant fields set that are extracted from the urn
   */
  @Nonnull
  private static DatasetDocument setUrnDerivedFields(@Nonnull DatasetUrn urn) {
    return new DatasetDocument()
        .setName(urn.getDatasetNameEntity())
        .setOrigin(urn.getOriginEntity())
        .setPlatform(urn.getPlatformEntity().getPlatformNameEntity())
        .setUrn(urn)
        .setBrowsePaths(new StringArray(Collections.singletonList(buildBrowsePath(urn))));
  }

  @Nonnull
  private DatasetDocument getDocumentToUpdateFromAspect(@Nonnull DatasetUrn urn, @Nonnull Ownership ownership) {
    final StringArray owners = BuilderUtils.getCorpUserOwners(ownership);
    return new DatasetDocument()
        .setUrn(urn)
        .setHasOwners(!owners.isEmpty())
        .setOwners(owners);
  }

  @Nonnull
  private DatasetDocument getDocumentToUpdateFromAspect(@Nonnull DatasetUrn urn, @Nonnull Status status) {
    return new DatasetDocument()
        .setUrn(urn)
        .setRemoved(status.isRemoved());
  }

  @Nonnull
  private DatasetDocument getDocumentToUpdateFromAspect(@Nonnull DatasetUrn urn, @Nonnull DatasetDeprecation deprecation) {
    return new DatasetDocument().setUrn(urn).setDeprecated(deprecation.isDeprecated());
  }

  @Nonnull
  private DatasetDocument getDocumentToUpdateFromAspect(@Nonnull DatasetUrn urn, @Nonnull DatasetProperties datasetProperties) {
    final DatasetDocument doc = new DatasetDocument().setUrn(urn);
    if (datasetProperties.getDescription() != null) {
      doc.setDescription(datasetProperties.getDescription());
    }
    return doc;
  }

  @Nonnull
  private DatasetDocument getDocumentToUpdateFromAspect(@Nonnull DatasetUrn urn, @Nonnull SchemaMetadata schemaMetadata) {
    return new DatasetDocument()
        .setUrn(urn)
        .setHasSchema(true);
  }

  @Nonnull
  private DatasetDocument getDocumentToUpdateFromAspect(@Nonnull DatasetUrn urn,
      @Nonnull UpstreamLineage upstreamLineage) {
    return new DatasetDocument().setUrn(urn)
        .setUpstreams(new DatasetUrnArray(upstreamLineage.getUpstreams()
            .stream()
            .map(upstream -> upstream.getDataset())
            .collect(Collectors.toList())));
  }

  @Nonnull
  private List<DatasetDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull DatasetSnapshot datasetSnapshot) {
    final DatasetUrn urn = datasetSnapshot.getUrn();
    final List<DatasetDocument> documents = datasetSnapshot.getAspects().stream().map(aspect -> {
      if (aspect.isDatasetDeprecation()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getDatasetDeprecation());
      } else if (aspect.isDatasetProperties()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getDatasetProperties());
      } else if (aspect.isOwnership()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getOwnership());
      } else if (aspect.isSchemaMetadata()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getSchemaMetadata());
      } else if (aspect.isStatus()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getStatus());
      } else if (aspect.isUpstreamLineage()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getUpstreamLineage());
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
    documents.add(setUrnDerivedFields(urn));
    return documents;
  }

  @Override
  @Nonnull
  public final List<DatasetDocument> getDocumentsToUpdate(@Nonnull RecordTemplate genericSnapshot) {
    if (genericSnapshot instanceof DatasetSnapshot) {
      return getDocumentsToUpdateFromSnapshotType((DatasetSnapshot) genericSnapshot);
    }
    return Collections.emptyList();
  }

  @Override
  @Nonnull
  public Class<DatasetDocument> getDocumentType() {
    return DatasetDocument.class;
  }
}
