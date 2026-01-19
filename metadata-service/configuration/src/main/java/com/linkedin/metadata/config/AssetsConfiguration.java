package com.linkedin.metadata.config;

import java.util.List;
import lombok.Data;

@Data
public class AssetsConfiguration {
  /** The url of the logo to render in the DataHub Application. */
  public String logoUrl;

  public List<String> externalScripts;
  public String faviconUrl;
}
