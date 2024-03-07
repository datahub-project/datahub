package io.datahubproject.openlineage.utils;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.metadata.key.DatasetKey;
import datahub.event.MetadataChangeProposalWrapper;

public class DatahubUtils {
  private DatahubUtils() {}

  public static DataFlowUrn flowUrn(String platformId, String appName) {
    return new DataFlowUrn("spark", appName, platformId);
  }

  public static DataJobUrn jobUrn(DataFlowUrn flowUrn, String jobName) {
    return new DataJobUrn(flowUrn, jobName);
  }

  public static MetadataChangeProposalWrapper generateDatasetMcp(DatasetUrn datasetUrn) {
    DatasetKey datasetAspect = new DatasetKey().setOrigin(FabricType.PROD);
    datasetAspect
        .setName(datasetUrn.getDatasetNameEntity())
        .setPlatform(new DataPlatformUrn(datasetUrn.getPlatformEntity().getPlatformNameEntity()));

    return MetadataChangeProposalWrapper.create(
        b -> b.entityType("dataset").entityUrn(datasetUrn).upsert().aspect(datasetAspect));
  }

  public static DatasetUrn createDatasetUrn(
      String platform, String platformInstance, String name, FabricType fabricType) {
    String datasteName = platformInstance == null ? name : platformInstance + "." + name;
    return new DatasetUrn(new DataPlatformUrn(platform), datasteName, fabricType);
  }
}
