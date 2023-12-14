package com.linkedin.datahub.graphql.analytics.service;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.BarSegment;
import com.linkedin.datahub.graphql.generated.Cell;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityProfileParams;
import com.linkedin.datahub.graphql.generated.LinkParams;
import com.linkedin.datahub.graphql.generated.NamedBar;
import com.linkedin.datahub.graphql.generated.Row;
import com.linkedin.datahub.graphql.generated.SearchParams;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.GlossaryTermKey;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class AnalyticsUtil {
  private AnalyticsUtil() {}

  public static Cell buildCellWithSearchLandingPage(String query) {
    Cell result = new Cell();
    result.setValue(query);
    result.setLinkParams(
        LinkParams.builder()
            .setSearchParams(SearchParams.builder().setQuery(query).build())
            .build());
    return result;
  }

  public static Cell buildCellWithEntityLandingPage(String urn) {
    Cell result = new Cell();
    result.setValue(urn);
    try {
      Entity entity = UrnToEntityMapper.map(Urn.createFromString(urn));
      result.setEntity(entity);
      result.setLinkParams(
          LinkParams.builder()
              .setEntityProfileParams(
                  EntityProfileParams.builder().setUrn(urn).setType(entity.getType()).build())
              .build());
    } catch (URISyntaxException e) {
      log.error("Malformed urn {} in table", urn, e);
    }
    return result;
  }

  public static void hydrateDisplayNameForBars(
      EntityClient entityClient,
      List<NamedBar> bars,
      String entityName,
      Set<String> aspectNames,
      Function<EntityResponse, Optional<String>> extractDisplayName,
      Authentication authentication)
      throws Exception {
    Map<String, String> urnToDisplayName =
        getUrnToDisplayName(
            entityClient,
            bars.stream().map(NamedBar::getName).collect(Collectors.toList()),
            entityName,
            aspectNames,
            extractDisplayName,
            authentication);
    // For each urn, try to find it's name, use the urn if not found
    bars.forEach(
        namedBar ->
            namedBar.setName(
                urnToDisplayName.getOrDefault(namedBar.getName(), namedBar.getName())));
  }

  public static void hydrateDisplayNameForSegments(
      EntityClient entityClient,
      List<NamedBar> bars,
      String entityName,
      Set<String> aspectNames,
      Function<EntityResponse, Optional<String>> extractDisplayName,
      Authentication authentication)
      throws Exception {
    Map<String, String> urnToDisplayName =
        getUrnToDisplayName(
            entityClient,
            bars.stream()
                .flatMap(bar -> bar.getSegments().stream().map(BarSegment::getLabel))
                .collect(Collectors.toList()),
            entityName,
            aspectNames,
            extractDisplayName,
            authentication);
    // For each urn, try to find it's name, use the urn if not found
    bars.forEach(
        namedBar ->
            namedBar
                .getSegments()
                .forEach(
                    segment ->
                        segment.setLabel(
                            urnToDisplayName.getOrDefault(
                                segment.getLabel(), segment.getLabel()))));
  }

  public static void hydrateDisplayNameForTable(
      EntityClient entityClient,
      List<Row> rows,
      String entityName,
      Set<String> aspectNames,
      Function<EntityResponse, Optional<String>> extractDisplayName,
      Authentication authentication)
      throws Exception {
    Map<String, String> urnToDisplayName =
        getUrnToDisplayName(
            entityClient,
            rows.stream()
                .flatMap(
                    row ->
                        row.getCells().stream()
                            .filter(cell -> cell.getEntity() != null)
                            .map(Cell::getValue))
                .collect(Collectors.toList()),
            entityName,
            aspectNames,
            extractDisplayName,
            authentication);
    // For each urn, try to find it's name, use the urn if not found
    rows.forEach(
        row ->
            row.getCells()
                .forEach(
                    cell -> {
                      if (cell.getEntity() != null) {
                        cell.setValue(
                            urnToDisplayName.getOrDefault(cell.getValue(), cell.getValue()));
                      }
                    }));
  }

  public static Map<String, String> getUrnToDisplayName(
      EntityClient entityClient,
      List<String> urns,
      String entityName,
      Set<String> aspectNames,
      Function<EntityResponse, Optional<String>> extractDisplayName,
      Authentication authentication)
      throws Exception {
    Set<Urn> uniqueUrns =
        urns.stream()
            .distinct()
            .map(
                urnStr -> {
                  try {
                    return Urn.createFromString(urnStr);
                  } catch (URISyntaxException e) {
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    Map<Urn, EntityResponse> aspects =
        entityClient.batchGetV2(entityName, uniqueUrns, aspectNames, authentication);
    return aspects.entrySet().stream()
        .map(
            entry -> Pair.of(entry.getKey().toString(), extractDisplayName.apply(entry.getValue())))
        .filter(pair -> pair.getValue().isPresent())
        .collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue().get()));
  }

  public static Optional<String> getDomainName(EntityResponse entityResponse) {
    EnvelopedAspect domainProperties =
        entityResponse.getAspects().get(Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    if (domainProperties == null) {
      return Optional.empty();
    }
    return Optional.of(new DomainProperties(domainProperties.getValue().data()).getName());
  }

  public static Optional<String> getPlatformName(EntityResponse entityResponse) {
    EnvelopedAspect envelopedDataPlatformInfo =
        entityResponse.getAspects().get(Constants.DATA_PLATFORM_INFO_ASPECT_NAME);
    if (envelopedDataPlatformInfo == null) {
      return Optional.empty();
    }
    DataPlatformInfo dataPlatformInfo =
        new DataPlatformInfo(envelopedDataPlatformInfo.getValue().data());
    return Optional.of(
        dataPlatformInfo.getDisplayName() == null
            ? dataPlatformInfo.getName()
            : dataPlatformInfo.getDisplayName());
  }

  public static Optional<String> getDatasetName(EntityResponse entityResponse) {
    EnvelopedAspect envelopedDatasetKey =
        entityResponse.getAspects().get(Constants.DATASET_KEY_ASPECT_NAME);
    if (envelopedDatasetKey == null) {
      return Optional.empty();
    }
    DatasetKey datasetKey = new DatasetKey(envelopedDatasetKey.getValue().data());
    return Optional.of(datasetKey.getName());
  }

  public static Optional<String> getTermName(EntityResponse entityResponse) {
    EnvelopedAspect envelopedTermInfo =
        entityResponse.getAspects().get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    if (envelopedTermInfo != null) {
      GlossaryTermInfo glossaryTermInfo = new GlossaryTermInfo(envelopedTermInfo.getValue().data());
      if (glossaryTermInfo.hasName()) {
        return Optional.ofNullable(glossaryTermInfo.getName());
      }
    }

    // if name is not set on GlossaryTermInfo or there is no GlossaryTermInfo
    EnvelopedAspect envelopedGlossaryTermKey =
        entityResponse.getAspects().get(Constants.GLOSSARY_TERM_KEY_ASPECT_NAME);
    if (envelopedGlossaryTermKey == null) {
      return Optional.empty();
    }
    GlossaryTermKey glossaryTermKey =
        new GlossaryTermKey(envelopedGlossaryTermKey.getValue().data());
    return Optional.of(glossaryTermKey.getName());
  }
}
