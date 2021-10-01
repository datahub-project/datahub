package com.linkedin.metadata.search.utils;

import com.linkedin.common.BrowsePaths;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.builders.search.ChartIndexBuilder;
import com.linkedin.metadata.builders.search.DashboardIndexBuilder;
import com.linkedin.metadata.builders.search.DataFlowIndexBuilder;
import com.linkedin.metadata.builders.search.DataJobIndexBuilder;
import com.linkedin.metadata.builders.search.DatasetIndexBuilder;
import com.linkedin.metadata.builders.search.GlossaryTermInfoIndexBuilder;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class BrowsePathUtils {
  private BrowsePathUtils() {
    //not called
  }

  public static BrowsePaths buildBrowsePath(Urn urn) throws URISyntaxException {
    String defaultBrowsePath = getDefaultBrowsePath(urn);
    StringArray browsePaths = new StringArray();
    browsePaths.add(defaultBrowsePath);
    BrowsePaths browsePathAspect = new BrowsePaths();
    browsePathAspect.setPaths(browsePaths);
    return browsePathAspect;
  }

  public static String getDefaultBrowsePath(Urn urn) throws URISyntaxException {
    switch (urn.getEntityType()) {
      case "dataset":
        return DatasetIndexBuilder.buildBrowsePath(DatasetUrn.createFromUrn(urn));
      case "chart":
        return ChartIndexBuilder.buildBrowsePath(ChartUrn.createFromUrn(urn));
      case "dashboard":
        return DashboardIndexBuilder.buildBrowsePath(DashboardUrn.createFromUrn(urn));
      case "dataFlow":
        return DataFlowIndexBuilder.buildBrowsePath(DataFlowUrn.createFromUrn(urn));
      case "dataJob":
        return DataJobIndexBuilder.buildBrowsePath(DataJobUrn.createFromUrn(urn));
      case "glossaryTerm":
        return GlossaryTermInfoIndexBuilder.buildBrowsePath(GlossaryTermUrn.createFromUrn(urn));
      default:
        log.debug(
            String.format("Failed to generate default browse path for unknown entity type %s", urn.getEntityType()));
        return "";
    }
  }
}
