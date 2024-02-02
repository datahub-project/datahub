package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;

import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataJobKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BrowsePathV2Utils {

  private static final String DEFAULT_FOLDER_NAME = "Default";

  /**
   * Generates a default browsePathsV2 aspect for a given urn.
   *
   * <p>If the entity has containers, get its whole container path and set those urns in the path of
   * browsePathsV2. If it's a dataset, generate the path from the dataset name like we do for
   * default browsePaths V1. If it's a data job, set its parent data flow in the path. For
   * everything else, place it in a "Default" folder so we can still navigate to it through browse
   * in the UI. This default method should be unneeded once ingestion produces higher quality
   * browsePathsV2 aspects.
   */
  public static BrowsePathsV2 getDefaultBrowsePathV2(
      @Nonnull Urn urn,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Character dataPlatformDelimiter,
      @Nonnull EntityService entityService,
      boolean useContainerPaths)
      throws URISyntaxException {

    BrowsePathsV2 result = new BrowsePathsV2();
    BrowsePathEntryArray browsePathEntries = new BrowsePathEntryArray();

    switch (urn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        DatasetKey dsKey =
            (DatasetKey)
                EntityKeyUtils.convertUrnToEntityKey(
                    urn, getKeyAspectSpec(urn.getEntityType(), entityRegistry));
        BrowsePathEntryArray datasetContainerPathEntries =
            useContainerPaths ? getContainerPathEntries(urn, entityService) : null;
        if (useContainerPaths && datasetContainerPathEntries.size() > 0) {
          browsePathEntries.addAll(datasetContainerPathEntries);
        } else {
          BrowsePathEntryArray defaultDatasetPathEntries =
              getDefaultDatasetPathEntries(dsKey.getName(), dataPlatformDelimiter);
          if (defaultDatasetPathEntries.size() > 0) {
            browsePathEntries.addAll(
                getDefaultDatasetPathEntries(dsKey.getName().toLowerCase(), dataPlatformDelimiter));
          } else {
            browsePathEntries.add(createBrowsePathEntry(DEFAULT_FOLDER_NAME, null));
          }
        }
        break;
        // Some sources produce charts and dashboards with containers. If we have containers, use
        // them, otherwise use default folder
      case Constants.CHART_ENTITY_NAME:
      case Constants.DASHBOARD_ENTITY_NAME:
        BrowsePathEntryArray containerPathEntries =
            useContainerPaths ? getContainerPathEntries(urn, entityService) : null;
        if (useContainerPaths && containerPathEntries.size() > 0) {
          browsePathEntries.addAll(containerPathEntries);
        } else {
          browsePathEntries.add(createBrowsePathEntry(DEFAULT_FOLDER_NAME, null));
        }
        break;
      case Constants.DATA_JOB_ENTITY_NAME:
        DataJobKey dataJobKey =
            (DataJobKey)
                EntityKeyUtils.convertUrnToEntityKey(
                    urn, getKeyAspectSpec(urn.getEntityType(), entityRegistry));
        browsePathEntries.add(
            createBrowsePathEntry(dataJobKey.getFlow().toString(), dataJobKey.getFlow()));
        break;
      default:
        browsePathEntries.add(createBrowsePathEntry(DEFAULT_FOLDER_NAME, null));
        break;
    }

    result.setPath(browsePathEntries);
    return result;
  }

  private static BrowsePathEntry createBrowsePathEntry(@Nonnull String id, @Nullable Urn urn) {
    BrowsePathEntry pathEntry = new BrowsePathEntry();
    pathEntry.setId(id);
    if (urn != null) {
      pathEntry.setUrn(urn);
    }
    return pathEntry;
  }

  private static void aggregateParentContainers(
      List<Urn> containerUrns, Urn entityUrn, EntityService entityService) {
    try {
      EntityResponse entityResponse =
          entityService.getEntityV2(
              entityUrn.getEntityType(), entityUrn, Collections.singleton(CONTAINER_ASPECT_NAME));

      if (entityResponse != null
          && entityResponse.getAspects().containsKey(CONTAINER_ASPECT_NAME)) {
        DataMap dataMap = entityResponse.getAspects().get(CONTAINER_ASPECT_NAME).getValue().data();
        com.linkedin.container.Container container = new com.linkedin.container.Container(dataMap);
        Urn containerUrn = container.getContainer();
        // add to beginning of the array, we want the highest level container first
        containerUrns.add(0, containerUrn);
        aggregateParentContainers(containerUrns, containerUrn, entityService);
      }
    } catch (Exception e) {
      log.error(
          String.format(
              "Error getting containers for entity with urn %s while adding default browsePathV2",
              entityUrn),
          e);
    }
  }

  /**
   * Gets the path of containers for a given entity to create a browsePathV2 off of. Recursively
   * call aggregateParentContainers to get the full container path to be included in this path.
   */
  private static BrowsePathEntryArray getContainerPathEntries(
      @Nonnull final Urn entityUrn, @Nonnull final EntityService entityService) {
    BrowsePathEntryArray browsePathEntries = new BrowsePathEntryArray();
    final List<Urn> containerUrns = new ArrayList<>();
    aggregateParentContainers(containerUrns, entityUrn, entityService);
    containerUrns.forEach(
        urn -> {
          browsePathEntries.add(createBrowsePathEntry(urn.toString(), urn));
        });
    return browsePathEntries;
  }

  /**
   * Attempts to convert a dataset name into a proper browse path by splitting it using the Data
   * Platform delimiter. If there are not > 1 name parts, then an empty string will be returned.
   */
  private static BrowsePathEntryArray getDefaultDatasetPathEntries(
      @Nonnull final String datasetName, @Nonnull final Character delimiter) {
    BrowsePathEntryArray browsePathEntries = new BrowsePathEntryArray();
    if (datasetName.contains(delimiter.toString())) {
      final List<String> datasetNamePathParts =
          Arrays.stream(datasetName.split(Pattern.quote(delimiter.toString())))
              .filter((name) -> !name.isEmpty())
              .collect(Collectors.toList());
      // Omit the name from the path.
      datasetNamePathParts
          .subList(0, datasetNamePathParts.size() - 1)
          .forEach(
              (part -> {
                browsePathEntries.add(createBrowsePathEntry(part, null));
              }));
    }
    return browsePathEntries;
  }

  protected static AspectSpec getKeyAspectSpec(
      final String entityName, final EntityRegistry registry) {
    final EntitySpec spec = registry.getEntitySpec(entityName);
    return spec.getKeyAspectSpec();
  }

  private BrowsePathV2Utils() {}
}
