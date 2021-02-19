package com.linkedin.datahub.dao.table;

import com.linkedin.dataPlatforms.DataPlatform;
import com.linkedin.dataplatform.client.DataPlatforms;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class DataPlatformsDao {

  private final DataPlatforms _dataPlatforms;

  public DataPlatformsDao(@Nonnull DataPlatforms dataPlatforms) {
    _dataPlatforms = dataPlatforms;
  }

  /**
   * Get all data platforms
   */
  public List<Map<String, Object>> getAllPlatforms() throws Exception {
    return _dataPlatforms.getAllPlatforms()
            .stream()
            .filter(DataPlatform::hasDataPlatformInfo)
            .map(platform -> platform.getDataPlatformInfo().data())
            .collect(Collectors.toList());
  }
}
