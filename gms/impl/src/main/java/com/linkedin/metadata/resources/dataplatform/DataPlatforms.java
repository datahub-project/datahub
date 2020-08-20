package com.linkedin.metadata.resources.dataplatform;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.metadata.aspect.DataPlatformAspect;
import com.linkedin.metadata.dao.ImmutableLocalDAO;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceAsyncTemplate;
import java.util.List;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;


/**
 * Resource provides information about various data platforms.
 */
@RestLiCollection(name = "dataPlatforms", namespace = "com.linkedin.dataplatform", keyName = "platformName")
public class DataPlatforms extends CollectionResourceAsyncTemplate<String, DataPlatformInfo> {

  @Inject
  @Named("dataPlatformLocalDAO")
  private ImmutableLocalDAO<DataPlatformAspect, DataPlatformUrn> _localDAO;

  @RestMethod.Get
  public Task<DataPlatformInfo> get(@Nonnull final String name) {
    return RestliUtils.toTaskFromOptional(() -> _localDAO.get(DataPlatformInfo.class, new DataPlatformUrn(name)));
  }

  @RestMethod.GetAll
  public Task<List<DataPlatformInfo>> getAll(@Nonnull @PagingContextParam(defaultCount = 100) PagingContext pagingContext) {
    return Task.value(_localDAO.list(DataPlatformInfo.class, pagingContext.getStart(), pagingContext.getCount()).getValues());
  }
}

