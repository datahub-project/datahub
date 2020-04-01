package com.linkedin.metadata.resources.identity;

import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import javax.annotation.Nonnull;


/**
 * Rest.li entry point: /corpUsers/{corpUserKey}/editableInfo
 */
@RestLiCollection(name = "editableInfo", namespace = "com.linkedin.identity", parent = CorpUsers.class)
public final class CorpUsersEditableInfoResource extends BaseCorpUsersAspectResource<CorpUserEditableInfo> {

  public CorpUsersEditableInfoResource() {
    super(CorpUserEditableInfo.class);
  }

  @Nonnull
  @Override
  @RestMethod.Create
  public Task<CreateResponse> create(@Nonnull CorpUserEditableInfo corpUserEditableInfo) {
    return super.create(corpUserEditableInfo);
  }

  @Nonnull
  @Override
  @RestMethod.Get
  public Task<CorpUserEditableInfo> get(@QueryParam("version") @Optional("0") @Nonnull Long version) {
    return super.get(version);
  }

}