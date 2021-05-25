package com.linkedin.metadata.resources.identity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import javax.annotation.Nonnull;


/**
 * Rest.li entry point: /corpUsers/{corpUserKey}/editableInfo
 */
@Deprecated
@RestLiCollection(name = "editableInfo", namespace = "com.linkedin.identity", parent = CorpUsers.class)
public final class CorpUsersEditableInfoResource extends BaseCorpUsersAspectResource<CorpUserEditableInfo> {

  public CorpUsersEditableInfoResource() {
    super(CorpUserEditableInfo.class);
  }

  @Nonnull
  @Override
  @RestMethod.Create
  public Task<CreateResponse> create(@Nonnull CorpUserEditableInfo corpUserEditableInfo) {
    return RestliUtils.toTask(() -> {
      final Urn urn = getUrn(getContext().getPathKeys());
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      getEntityService().ingestAspect(
          urn,
          PegasusUtils.getAspectNameFromSchema(corpUserEditableInfo.schema()),
          corpUserEditableInfo,
          auditStamp);
      return new CreateResponse(HttpStatus.S_201_CREATED);
    });
  }

  @Nonnull
  @Override
  @RestMethod.Get
  public Task<CorpUserEditableInfo> get(@QueryParam("version") @Optional("0") @Nonnull Long version) {
    return RestliUtils.toTask(() -> {
      final Urn urn = getUrn(getContext().getPathKeys());
      final RecordDataSchema aspectSchema = new CorpUserEditableInfo().schema();

      final RecordTemplate maybeAspect = getEntityService().getAspect(
          urn,
          PegasusUtils.getAspectNameFromSchema(aspectSchema),
          version
      );
      if (maybeAspect != null) {
        return new CorpUserEditableInfo(maybeAspect.data());
      }
      throw RestliUtils.resourceNotFoundException();
    });
  }
}