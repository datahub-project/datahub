package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.InstitutionalMemoryMetadata;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class InstitutionalMemoryMetadataMapper {

  public static final InstitutionalMemoryMetadataMapper INSTANCE =
      new InstitutionalMemoryMetadataMapper();

  public static InstitutionalMemoryMetadata map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.InstitutionalMemoryMetadata metadata,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, metadata, entityUrn);
  }

  public InstitutionalMemoryMetadata apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.InstitutionalMemoryMetadata input,
      @Nonnull final Urn entityUrn) {
    final InstitutionalMemoryMetadata result = new InstitutionalMemoryMetadata();
    result.setUrl(input.getUrl().toString());
    result.setDescription(input.getDescription()); // deprecated field
    result.setLabel(input.getDescription());
    result.setAuthor(getAuthor(input.getCreateStamp().getActor().toString()));
    result.setActor(ResolvedActorMapper.map(input.getCreateStamp().getActor()));
    result.setCreated(AuditStampMapper.map(context, input.getCreateStamp()));
    result.setAssociatedUrn(entityUrn.toString());
    return result;
  }

  private CorpUser getAuthor(String actor) {
    CorpUser partialUser = new CorpUser();
    partialUser.setUrn(actor);
    return partialUser;
  }
}
