package com.linkedin.metadata.resources.dataplatform.utils;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.metadata.dao.ImmutableLocalDAO;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.json.simple.parser.ParseException;


public class DataPlatformsUtil {

  private DataPlatformsUtil() {
  }

  private static final Map<DataPlatformUrn, DataPlatformInfo> DATA_PLATFORM_INFO_MAP =
      loadAspectsFromResource("DataPlatformInfo.json");

  public static Map<DataPlatformUrn, DataPlatformInfo> getDataPlatformInfoMap() {
    return DATA_PLATFORM_INFO_MAP;
  }

  private static Map<DataPlatformUrn, DataPlatformInfo> loadAspectsFromResource(
      @Nonnull final String resource) {
    try {
      return ImmutableLocalDAO.loadAspects(DataPlatformInfo.class,
          DataPlatformsUtil.class.getClassLoader().getResourceAsStream(resource));
    } catch (ParseException | IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean isValidDataPlatform(String platformName) {
    return DATA_PLATFORM_INFO_MAP.containsKey(toDataPlatformUrn(platformName));
  }

  @Nonnull
  public static Optional<DataPlatformInfo> get(@Nonnull String platformName) {
    return Optional.ofNullable(DATA_PLATFORM_INFO_MAP.get(toDataPlatformUrn(platformName)));
  }

  @Nonnull
  public static Optional<String> getPlatformDelimiter(@Nonnull String platformName) {
    return get(platformName).map(DataPlatformInfo::getDatasetNameDelimiter);
  }

  private static DataPlatformUrn toDataPlatformUrn(@Nonnull String platformName) {
    return new DataPlatformUrn(platformName);
  }
}
