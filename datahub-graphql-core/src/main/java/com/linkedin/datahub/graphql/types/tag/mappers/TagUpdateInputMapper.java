package com.linkedin.datahub.graphql.types.tag.mappers;

import static com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.TagUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.tag.TagProperties;
import java.util.ArrayList;
import java.util.Collection;
import javax.annotation.Nonnull;

public class TagUpdateInputMapper
    implements InputModelMapper<TagUpdateInput, Collection<MetadataChangeProposal>, Urn> {

  public static final TagUpdateInputMapper INSTANCE = new TagUpdateInputMapper();

  public static Collection<MetadataChangeProposal> map(
      @Nonnull final TagUpdateInput tagUpdate, @Nonnull final Urn actor) {
    return INSTANCE.apply(tagUpdate, actor);
  }

  @Override
  public Collection<MetadataChangeProposal> apply(
      @Nonnull final TagUpdateInput tagUpdate, @Nonnull final Urn actor) {
    final Collection<MetadataChangeProposal> proposals = new ArrayList<>(2);
    final UpdateMappingHelper updateMappingHelper = new UpdateMappingHelper(TAG_ENTITY_NAME);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    // Creator is the owner.
    final Ownership ownership = new Ownership();
    final Owner owner = new Owner();
    owner.setOwner(actor);
    owner.setType(OwnershipType.NONE);
    owner.setTypeUrn(UrnUtils.getUrn(mapOwnershipTypeToEntity(OwnershipType.NONE.name())));
    owner.setSource(new OwnershipSource().setType(OwnershipSourceType.SERVICE));
    ownership.setOwners(new OwnerArray(owner));
    ownership.setLastModified(auditStamp);
    proposals.add(updateMappingHelper.aspectToProposal(ownership, OWNERSHIP_ASPECT_NAME));

    if (tagUpdate.getName() != null || tagUpdate.getDescription() != null) {
      TagProperties tagProperties = new TagProperties();
      tagProperties.setName(tagUpdate.getName());
      tagProperties.setDescription(tagUpdate.getDescription());
      proposals.add(
          updateMappingHelper.aspectToProposal(tagProperties, TAG_PROPERTIES_ASPECT_NAME));
    }

    return proposals;
  }
}
