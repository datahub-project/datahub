package com.linkedin.metadata;

import com.linkedin.chart.ChartInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.transactions.UpsertBatchItem;
import com.linkedin.metadata.key.CorpUserKey;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

public class AspectIngestionUtils {

  private AspectIngestionUtils() {}

  @Nonnull
  public static Map<Urn, CorpUserKey> ingestCorpUserKeyAspects(
      EntityService entityService, int aspectCount) {
    return ingestCorpUserKeyAspects(entityService, aspectCount, 0);
  }

  @Nonnull
  public static Map<Urn, CorpUserKey> ingestCorpUserKeyAspects(
      EntityService entityService, int aspectCount, int startIndex) {
    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserKey());
    Map<Urn, CorpUserKey> aspects = new HashMap<>();
    List<UpsertBatchItem> items = new LinkedList<>();
    for (int i = startIndex; i < startIndex + aspectCount; i++) {
      Urn urn = UrnUtils.getUrn(String.format("urn:li:corpuser:tester%d", i));
      CorpUserKey aspect = AspectGenerationUtils.createCorpUserKey(urn);
      aspects.put(urn, aspect);
      items.add(
          UpsertBatchItem.builder()
              .urn(urn)
              .aspectName(aspectName)
              .aspect(aspect)
              .systemMetadata(AspectGenerationUtils.createSystemMetadata())
              .build(entityService.getEntityRegistry()));
    }
    entityService.ingestAspects(
        AspectsBatchImpl.builder().items(items).build(),
        AspectGenerationUtils.createAuditStamp(),
        true,
        true);
    return aspects;
  }

  @Nonnull
  public static Map<Urn, CorpUserInfo> ingestCorpUserInfoAspects(
      @Nonnull final EntityService entityService, int aspectCount) {
    return ingestCorpUserInfoAspects(entityService, aspectCount, 0);
  }

  @Nonnull
  public static Map<Urn, CorpUserInfo> ingestCorpUserInfoAspects(
      @Nonnull final EntityService entityService, int aspectCount, int startIndex) {
    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserInfo());
    Map<Urn, CorpUserInfo> aspects = new HashMap<>();
    List<UpsertBatchItem> items = new LinkedList<>();
    for (int i = startIndex; i < startIndex + aspectCount; i++) {
      Urn urn = UrnUtils.getUrn(String.format("urn:li:corpuser:tester%d", i));
      String email = String.format("email%d@test.com", i);
      CorpUserInfo aspect = AspectGenerationUtils.createCorpUserInfo(email);
      aspects.put(urn, aspect);
      items.add(
          UpsertBatchItem.builder()
              .urn(urn)
              .aspectName(aspectName)
              .aspect(aspect)
              .systemMetadata(AspectGenerationUtils.createSystemMetadata())
              .build(entityService.getEntityRegistry()));
    }
    entityService.ingestAspects(
        AspectsBatchImpl.builder().items(items).build(),
        AspectGenerationUtils.createAuditStamp(),
        true,
        true);
    return aspects;
  }

  @Nonnull
  public static Map<Urn, ChartInfo> ingestChartInfoAspects(
      @Nonnull final EntityService entityService, int aspectCount) {
    return ingestChartInfoAspects(entityService, aspectCount, 0);
  }

  @Nonnull
  public static Map<Urn, ChartInfo> ingestChartInfoAspects(
      @Nonnull final EntityService entityService, int aspectCount, int startIndex) {
    String aspectName = AspectGenerationUtils.getAspectName(new ChartInfo());
    Map<Urn, ChartInfo> aspects = new HashMap<>();
    List<UpsertBatchItem> items = new LinkedList<>();
    for (int i = startIndex; i < startIndex + aspectCount; i++) {
      Urn urn = UrnUtils.getUrn(String.format("urn:li:chart:(looker,test%d)", i));
      String title = String.format("Test Title %d", i);
      String description = String.format("Test description %d", i);
      ChartInfo aspect = AspectGenerationUtils.createChartInfo(title, description);
      aspects.put(urn, aspect);
      items.add(
          UpsertBatchItem.builder()
              .urn(urn)
              .aspectName(aspectName)
              .aspect(aspect)
              .systemMetadata(AspectGenerationUtils.createSystemMetadata())
              .build(entityService.getEntityRegistry()));
    }
    entityService.ingestAspects(
        AspectsBatchImpl.builder().items(items).build(),
        AspectGenerationUtils.createAuditStamp(),
        true,
        true);
    return aspects;
  }
}
