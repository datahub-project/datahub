package com.linkedin.datahub.graphql.analytics.service;

import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dashboard.DashboardInfo;
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
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.GlossaryTermKey;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
      Entity entity = UrnToEntityMapper.map(null, Urn.createFromString(urn));
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
      @Nonnull OperationContext opContext,
      EntityClient entityClient,
      List<NamedBar> bars,
      String entityName,
      Set<String> aspectNames,
      Function<EntityResponse, Optional<String>> extractDisplayName)
      throws Exception {
    Map<String, String> urnToDisplayName =
        getUrnToDisplayName(
            opContext,
            entityClient,
            bars.stream().map(NamedBar::getName).collect(Collectors.toList()),
            entityName,
            aspectNames,
            extractDisplayName);
    // For each urn, try to find it's name, use the urn if not found
    bars.forEach(
        namedBar ->
            namedBar.setName(
                urnToDisplayName.getOrDefault(namedBar.getName(), namedBar.getName())));
  }

  public static void hydrateDisplayNameForSegments(
      @Nonnull OperationContext opContext,
      EntityClient entityClient,
      List<NamedBar> bars,
      String entityName,
      Set<String> aspectNames,
      Function<EntityResponse, Optional<String>> extractDisplayName)
      throws Exception {
    Map<String, String> urnToDisplayName =
        getUrnToDisplayName(
            opContext,
            entityClient,
            bars.stream()
                .flatMap(bar -> bar.getSegments().stream().map(BarSegment::getLabel))
                .collect(Collectors.toList()),
            entityName,
            aspectNames,
            extractDisplayName);
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
      @Nonnull OperationContext opContext,
      EntityClient entityClient,
      List<Row> rows,
      String entityName,
      Set<String> aspectNames,
      Function<EntityResponse, Optional<String>> extractDisplayName)
      throws Exception {
    Map<String, String> urnToDisplayName =
        getUrnToDisplayName(
            opContext,
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
            extractDisplayName);
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

  public static void convertToUserInfoRows(
      OperationContext opContext, EntityClient entityClient, List<Row> rows) throws Exception {
    final Set<Urn> userUrns =
        rows.stream()
            .filter(row -> !row.getCells().isEmpty())
            .map(row -> UrnUtils.getUrn(row.getCells().get(0).getValue()))
            .collect(Collectors.toSet());
    final Map<Urn, EntityResponse> gmsResponseByUser =
        entityClient.batchGetV2(
            opContext,
            CORP_USER_INFO_ASPECT_NAME,
            userUrns,
            ImmutableSet.of(CORP_USER_INFO_ASPECT_NAME));
    final Map<Urn, CorpUserInfo> urnToCorpUserInfo =
        gmsResponseByUser.entrySet().stream()
            .filter(
                entry ->
                    entry.getValue() != null
                        && entry.getValue().getAspects().containsKey(CORP_USER_INFO_ASPECT_NAME))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        new CorpUserInfo(
                            entry
                                .getValue()
                                .getAspects()
                                .get(CORP_USER_INFO_ASPECT_NAME)
                                .getValue()
                                .data())));
    // Populate a row with the user link, title, and email.
    rows.forEach(
        row -> {
          Urn urn = UrnUtils.getUrn(row.getCells().get(0).getValue());
          EntityResponse response = gmsResponseByUser.get(urn);
          String maybeDisplayName = response != null ? getUserName(response).orElse(null) : null;
          String maybeEmail =
              urnToCorpUserInfo.containsKey(urn) ? urnToCorpUserInfo.get(urn).getEmail() : null;
          String maybeTitle =
              urnToCorpUserInfo.containsKey(urn) ? urnToCorpUserInfo.get(urn).getTitle() : null;
          if (maybeDisplayName != null) {
            row.getCells().get(0).setValue(maybeDisplayName);
          }
          final List<Cell> newCells = new ArrayList<>();
          // First add the user cell
          newCells.add(row.getCells().get(0));
          // Then, add the title row.
          newCells.add(new Cell(maybeTitle != null ? maybeTitle : "None", null, null));
          // Finally, add the email row.
          newCells.add(new Cell(maybeEmail != null ? maybeEmail : "None", null, null));
          row.setCells(newCells);
        });
  }

  public static Map<String, String> getUrnToDisplayName(
      @Nonnull OperationContext opContext,
      EntityClient entityClient,
      List<String> urns,
      String entityName,
      Set<String> aspectNames,
      Function<EntityResponse, Optional<String>> extractDisplayName)
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
        entityClient.batchGetV2(opContext, entityName, uniqueUrns, aspectNames);
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
      return Optional.of(entityResponse.getUrn().getId());
    }
    DataPlatformInfo dataPlatformInfo =
        new DataPlatformInfo(envelopedDataPlatformInfo.getValue().data());
    final String infoDisplayName =
        dataPlatformInfo.getDisplayName() == null
            ? dataPlatformInfo.getName()
            : dataPlatformInfo.getDisplayName();
    return Optional.of(infoDisplayName != null ? infoDisplayName : entityResponse.getUrn().getId());
  }

  public static Optional<String> getDatasetName(EntityResponse entityResponse) {
    EnvelopedAspect envelopedDatasetProperties =
        entityResponse.getAspects().get(Constants.DATASET_PROPERTIES_ASPECT_NAME);
    if (envelopedDatasetProperties == null) {
      return Optional.empty();
    }
    DatasetProperties datasetProperties =
        new DatasetProperties(envelopedDatasetProperties.getValue().data());
    return Optional.of(
        datasetProperties.hasName()
            ? datasetProperties.getName()
            : entityResponse.getUrn().getEntityKey().get(1));
  }

  public static Optional<String> getDashboardName(EntityResponse entityResponse) {
    EnvelopedAspect envelopedDashboardName =
        entityResponse.getAspects().get(Constants.DASHBOARD_INFO_ASPECT_NAME);
    if (envelopedDashboardName == null) {
      return Optional.empty();
    }
    DashboardInfo dashboardInfo = new DashboardInfo(envelopedDashboardName.getValue().data());
    return Optional.of(dashboardInfo.getTitle());
  }

  public static Optional<String> getUserName(EntityResponse entityResponse) {
    EnvelopedAspect envelopedCorpUserInfo =
        entityResponse.getAspects().get(CORP_USER_INFO_ASPECT_NAME);
    if (envelopedCorpUserInfo == null) {
      return Optional.of(entityResponse.getUrn().getId());
    }
    CorpUserInfo corpUserInfo = new CorpUserInfo(envelopedCorpUserInfo.getValue().data());
    final String userInfoName =
        corpUserInfo.hasDisplayName()
            ? corpUserInfo.getDisplayName()
            : getUserFullName(corpUserInfo.getFirstName(), corpUserInfo.getLastName());
    return Optional.of(userInfoName != null ? userInfoName : entityResponse.getUrn().getId());
  }

  @Nullable
  private static String getUserFullName(
      @Nullable final String firstName, @Nullable final String lastName) {
    if (firstName != null && lastName != null) {
      return firstName + " " + lastName;
    }
    return null;
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
