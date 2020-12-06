package com.linkedin.metadata.resources.dataplatform;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.metadata.aspect.DataPlatformAspect;
import com.linkedin.metadata.aspect.DataPlatformAspectArray;
import com.linkedin.metadata.dao.ImmutableLocalDAO;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.snapshot.DataPlatformSnapshot;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceAsyncTemplate;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * Resource provides information about various data platforms.
 */
@RestLiCollection(name = "dataPlatforms", namespace = "com.linkedin.dataplatform", keyName = "platformName")
public class DataPlatforms
    extends CollectionResourceAsyncTemplate<String, DataPlatformInfo> {

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

  @Action(name = ACTION_GET_SNAPSHOT)
  @Nonnull
  public Task<DataPlatformSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) throws URISyntaxException {
    final DataPlatformInfo aspect = _localDAO.get(DataPlatformInfo.class, DataPlatformUrn.deserialize(urnString)).get();
    final DataPlatformAspect dataPlatformAspect = new DataPlatformAspect();
    dataPlatformAspect.setDataPlatformInfo(aspect);
    return Task.value(new DataPlatformSnapshot().setUrn(DataPlatformUrn.deserialize(urnString))
        .setAspects(new DataPlatformAspectArray(dataPlatformAspect)));
  }
}

