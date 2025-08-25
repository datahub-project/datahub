package com.linkedin.datahub.graphql.types.corpuser.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.generated.CorpUserInvitationStatus;
import com.linkedin.datahub.graphql.generated.InvitationStatus;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CorpUserInvitationStatusMapper
    implements ModelMapper<
        com.linkedin.identity.CorpUserInvitationStatus, CorpUserInvitationStatus> {

  public static final CorpUserInvitationStatusMapper INSTANCE =
      new CorpUserInvitationStatusMapper();

  public static CorpUserInvitationStatus map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.identity.CorpUserInvitationStatus corpUserInvitationStatus) {
    return INSTANCE.apply(context, corpUserInvitationStatus);
  }

  @Override
  public CorpUserInvitationStatus apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.identity.CorpUserInvitationStatus gmsInvitationStatus) {
    final CorpUserInvitationStatus result = new CorpUserInvitationStatus();

    // Map the status - convert from GMS enum to GraphQL enum
    result.setStatus(InvitationStatus.valueOf(gmsInvitationStatus.getStatus().name()));

    // Map audit stamps
    if (gmsInvitationStatus.hasCreated()) {
      AuditStamp createdStamp = AuditStampMapper.map(context, gmsInvitationStatus.getCreated());
      result.setCreated(createdStamp);
    }

    if (gmsInvitationStatus.hasLastUpdated()) {
      AuditStamp lastUpdatedStamp =
          AuditStampMapper.map(context, gmsInvitationStatus.getLastUpdated());
      result.setLastUpdated(lastUpdatedStamp);
    }

    // Map role URN if present
    if (gmsInvitationStatus.hasRole()) {
      result.setRole(gmsInvitationStatus.getRole().toString());
    }

    // Map invitation token
    if (gmsInvitationStatus.hasInvitationToken()) {
      result.setInvitationToken(gmsInvitationStatus.getInvitationToken());
    }

    return result;
  }
}
