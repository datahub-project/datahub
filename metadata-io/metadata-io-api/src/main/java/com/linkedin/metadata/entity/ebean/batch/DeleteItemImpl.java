package com.linkedin.metadata.entity.ebean.batch;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.entity.EntityApiUtils;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class DeleteItemImpl implements ChangeMCP {

  // urn an urn associated with the new aspect
  @Nonnull private final Urn urn;

  // aspectName name of the aspect being inserted
  @Nonnull private final String aspectName;

  @Nonnull private final AuditStamp auditStamp;

  // derived
  @Nonnull private final EntitySpec entitySpec;
  @Nonnull private final AspectSpec aspectSpec;

  @Setter @Nullable private SystemAspect previousSystemAspect;

  @Nonnull
  @Override
  public ChangeType getChangeType() {
    return ChangeType.DELETE;
  }

  @Nullable
  @Override
  public RecordTemplate getRecordTemplate() {
    return null;
  }

  @Nullable
  @Override
  public SystemMetadata getSystemMetadata() {
    return null;
  }

  @Nullable
  @Override
  public MetadataChangeProposal getMetadataChangeProposal() {
    return EntityApiUtils.buildMCP(getUrn(), aspectName, getChangeType(), null);
  }

  @Nonnull
  @Override
  public SystemAspect getSystemAspect(@Nullable Long nextAspectVersion) {
    EntityAspect entityAspect = new EntityAspect();
    entityAspect.setAspect(getAspectName());
    entityAspect.setUrn(getUrn().toString());
    entityAspect.setVersion(0);
    return EntityAspect.EntitySystemAspect.builder()
        .build(getEntitySpec(), getAspectSpec(), entityAspect);
  }

  @Override
  public long getNextAspectVersion() {
    return 0;
  }

  @Override
  public void setNextAspectVersion(long nextAspectVersion) {
    throw new IllegalStateException("Next aspect version is always zero");
  }

  public static class DeleteItemImplBuilder {

    // Ensure use of other builders
    private DeleteItemImpl build() {
      return null;
    }

    @SneakyThrows
    public DeleteItemImpl build(AspectRetriever aspectRetriever) {
      ValidationApiUtils.validateUrn(aspectRetriever.getEntityRegistry(), this.urn);
      log.debug("entity type = {}", this.urn.getEntityType());

      entitySpec(aspectRetriever.getEntityRegistry().getEntitySpec(this.urn.getEntityType()));
      log.debug("entity spec = {}", this.entitySpec);

      aspectSpec(ValidationApiUtils.validate(this.entitySpec, this.aspectName));
      log.debug("aspect spec = {}", this.aspectSpec);

      return new DeleteItemImpl(
          this.urn,
          this.aspectName,
          this.auditStamp,
          this.entitySpec,
          this.aspectSpec,
          this.previousSystemAspect);
    }
  }

  @Override
  public boolean isDatabaseDuplicateOf(BatchItem other) {
    return equals(other);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeleteItemImpl that = (DeleteItemImpl) o;
    return urn.equals(that.urn) && aspectName.equals(that.aspectName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(urn, aspectName);
  }

  @Override
  public String toString() {
    return "UpsertBatchItem{" + "urn=" + urn + ", aspectName='" + aspectName + '\'' + '}';
  }
}
