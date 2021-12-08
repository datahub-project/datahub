package com.linkedin.datahub.graphql.types.tag.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.TagUpdateInput;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.metadata.aspect.TagAspect;
import com.linkedin.metadata.aspect.TagAspectArray;
import com.linkedin.metadata.snapshot.TagSnapshot;

import com.linkedin.tag.TagProperties;
import javax.annotation.Nonnull;

public class TagUpdateInputSnapshotMapper implements InputModelMapper<TagUpdateInput, TagSnapshot, Urn> {

  public static final TagUpdateInputSnapshotMapper INSTANCE = new TagUpdateInputSnapshotMapper();

  public static TagSnapshot map(
      @Nonnull final TagUpdateInput tagUpdate,
      @Nonnull final Urn actor) {
    return INSTANCE.apply(tagUpdate, actor);
  }

  @Override
  public TagSnapshot apply(
      @Nonnull final TagUpdateInput tagUpdate,
      @Nonnull final Urn actor) {
    final TagSnapshot result = new TagSnapshot();
    result.setUrn((new TagUrn(tagUpdate.getName())));

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    final TagAspectArray aspects = new TagAspectArray();

    // Creator is the owner.
    final Ownership ownership = new Ownership();
    final Owner owner = new Owner();
    owner.setOwner(actor);
    owner.setType(OwnershipType.DATAOWNER);
    owner.setSource(new OwnershipSource().setType(OwnershipSourceType.SERVICE));
    ownership.setOwners(new OwnerArray(owner));
    ownership.setLastModified(auditStamp);
    aspects.add(TagAspect.create(ownership));

    if (tagUpdate.getName() != null || tagUpdate.getDescription() != null) {
      TagProperties tagProperties = new TagProperties();
      tagProperties.setName(tagUpdate.getName());
      tagProperties.setDescription(tagUpdate.getDescription());
      aspects.add(TagAspect.create(tagProperties));
    }

    result.setAspects(aspects);

    return result;
  }
}