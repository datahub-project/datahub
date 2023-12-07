package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class AssetsConfiguration {
  /** The url of the logo to render in the DataHub Application. */
  public String logoUrl;

  public String faviconUrl;
}
