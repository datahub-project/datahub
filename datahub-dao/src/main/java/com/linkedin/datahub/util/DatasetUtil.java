package com.linkedin.datahub.util;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.datahub.models.view.DatasetView;
import com.linkedin.datahub.models.view.LineageView;
import com.linkedin.dataset.Dataset;

import javax.annotation.Nonnull;
import java.net.URISyntaxException;

import static com.linkedin.datahub.util.UrnUtil.splitWhUrn;


public class DatasetUtil {
  private DatasetUtil() {

  }

  /**
   * Convert WhereHows dataset URN to DatasetUrn, set dataset origin as PROD.
   * @param urn String WH dataset URN
   * @return DatasetUrn
   */
  public static DatasetUrn toDatasetUrnFromWhUrn(@Nonnull String urn) {
    String[] urnParts = splitWhUrn(urn);
    return com.linkedin.common.urn.UrnUtils.toDatasetUrn(urnParts[0], urnParts[1], "PROD");
  }

  /**
   * Check input string to determine if WH URN or TMS URN, then convert to DatasetUrn
   */
  public static DatasetUrn toDatasetUrn(@Nonnull String datasetUrn) throws URISyntaxException {
    if (datasetUrn.contains(":///")) { // wherehows URN
      return toDatasetUrnFromWhUrn(datasetUrn);
    } else {  // TMS URN
      return DatasetUrn.createFromString(datasetUrn);
    }
  }

  /**
   * Convert TMS Dataset to WH DatasetView
   * @param dataset Dataset
   * @return DatasetView
   */
  public static DatasetView toDatasetView(Dataset dataset) {
    DatasetView view = new DatasetView();
    view.setPlatform(dataset.getPlatform().getContent());
    view.setNativeName(dataset.getName());
    view.setFabric(dataset.getOrigin().name());
    view.setDescription(dataset.getDescription());
    view.setTags(dataset.getTags());
    // construct DatasetUrn and overwrite URI field for frontend use
    view.setUri(new DatasetUrn(dataset.getPlatform(), dataset.getName(), dataset.getOrigin()).toString());

    if (dataset.hasPlatformNativeType()) {
      view.setNativeType(dataset.getPlatformNativeType().name());
    }
    if (dataset.getStatus() != null) {
      view.setRemoved(dataset.getStatus().isRemoved());
    }
    if (dataset.hasDeprecation()) {
      view.setDeprecated(dataset.getDeprecation().isDeprecated());
      view.setDeprecationNote(dataset.getDeprecation().getNote());
      if (dataset.getDeprecation().hasDecommissionTime()) {
        view.setDecommissionTime(dataset.getDeprecation().getDecommissionTime());
      }
    }
    if (dataset.hasCreated()) {
      view.setCreatedTime(dataset.getCreated().getTime());
    }
    if (dataset.hasLastModified()) {
      view.setModifiedTime(dataset.getLastModified().getTime());
    }
    if (dataset.hasProperties()) {
      view.setCustomProperties(dataset.getProperties());
    }
    return view;
  }

  /**
   * Converts TMS lineage response to WH LineageView which requires datasetView conversion
   * for the dataset in the lineage response
   * @param dataset dataset
   * @param lineageType type of lineage
   * @param auditStamp audit stamp
   * @return LineageView
   */
  public static LineageView toLineageView(Dataset dataset, String lineageType, AuditStamp auditStamp) {
    LineageView view = new LineageView();

    DatasetView datasetView = toDatasetView(dataset);
    datasetView.setModifiedTime(auditStamp.getTime());

    view.setDataset(datasetView);
    view.setType(lineageType);
    view.setActor(auditStamp.getActor().toString());

    return view;
  }
}
