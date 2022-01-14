package com.linkedin.datahub.lineage.spark.model.dataset;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class JdbcDataset implements SparkDataset {
  private final DatasetUrn urn;

  public JdbcDataset(String url, String tbl) {
    this.urn = new DatasetUrn(new DataPlatformUrn(platformName(url)), dsName(url, tbl), FabricType.PROD);
  }

  @Override
  public DatasetUrn urn() {
    return this.urn;
  }

  private static String platformName(String url) {
    if (url.contains("postgres")) {
      return "postgres";
    }
    return "unknownJdbc";
  }

  private static String dsName(String url, String tbl) {
    url = url.replaceFirst("jdbc:", "");
    if (url.contains("postgres")) {
      url = url.substring(url.lastIndexOf('/') + 1);
      url = url.substring(0, url.indexOf('?'));
    }
    // TODO different DBs have different formats. TBD mapping to data source names
    return url + "." + tbl;
  }
}
