package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.OwnershipUpdate;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.NonNull;

public class OwnershipUpdateMapper implements InputModelMapper<OwnershipUpdate, Ownership, Urn> {

  private static final OwnershipUpdateMapper INSTANCE = new OwnershipUpdateMapper();

  public static Ownership map(
      @Nullable QueryContext context,
      @NonNull final OwnershipUpdate input,
      @NonNull final Urn actor) {
    return INSTANCE.apply(context, input, actor);
  }

  @Override
  public Ownership apply(
      @Nullable QueryContext context,
      @NonNull final OwnershipUpdate input,
      @NonNull final Urn actor) {
    final Ownership ownership = new Ownership();

    ownership.setOwners(
        new OwnerArray(
            input.getOwners().stream()
                .map(o -> OwnerUpdateMapper.map(context, o))
                .collect(Collectors.toList())));

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());
    ownership.setLastModified(auditStamp);

    return ownership;
  }
}
