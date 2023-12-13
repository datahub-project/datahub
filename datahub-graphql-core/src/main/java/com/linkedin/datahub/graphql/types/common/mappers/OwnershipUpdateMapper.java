package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.OwnershipUpdate;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import java.util.stream.Collectors;
import lombok.NonNull;

public class OwnershipUpdateMapper implements InputModelMapper<OwnershipUpdate, Ownership, Urn> {

  private static final OwnershipUpdateMapper INSTANCE = new OwnershipUpdateMapper();

  public static Ownership map(@NonNull final OwnershipUpdate input, @NonNull final Urn actor) {
    return INSTANCE.apply(input, actor);
  }

  @Override
  public Ownership apply(@NonNull final OwnershipUpdate input, @NonNull final Urn actor) {
    final Ownership ownership = new Ownership();

    ownership.setOwners(
        new OwnerArray(
            input.getOwners().stream().map(OwnerUpdateMapper::map).collect(Collectors.toList())));

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());
    ownership.setLastModified(auditStamp);

    return ownership;
  }
}
