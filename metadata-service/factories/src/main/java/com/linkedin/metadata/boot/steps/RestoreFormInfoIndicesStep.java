package com.linkedin.metadata.boot.steps;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.query.ExtraInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreFormInfoIndicesStep extends UpgradeStep {
  private static final String VERSION = "1";
  private static final String UPGRADE_ID = "restore-form-info-indices";
  private static final Integer BATCH_SIZE = 1000;

  public RestoreFormInfoIndicesStep(@Nonnull final EntityService<?> entityService) {
    super(entityService, VERSION, UPGRADE_ID);
  }

  @Override
  public void upgrade(@Nonnull OperationContext systemOperationContext) throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    final int totalFormCount = getAndRestoreFormInfoIndices(systemOperationContext, 0, auditStamp);
    int formCount = BATCH_SIZE;
    while (formCount < totalFormCount) {
      getAndRestoreFormInfoIndices(systemOperationContext, formCount, auditStamp);
      formCount += BATCH_SIZE;
    }
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  private int getAndRestoreFormInfoIndices(
      @Nonnull OperationContext systemOperationContext, int start, AuditStamp auditStamp) {
    final AspectSpec formInfoAspectSpec =
        systemOperationContext
            .getEntityRegistry()
            .getEntitySpec(Constants.FORM_ENTITY_NAME)
            .getAspectSpec(Constants.FORM_INFO_ASPECT_NAME);

    final ListResult<RecordTemplate> latestAspects =
        entityService.listLatestAspects(
            systemOperationContext,
            Constants.FORM_ENTITY_NAME,
            Constants.FORM_INFO_ASPECT_NAME,
            start,
            BATCH_SIZE);

    if (latestAspects.getTotalCount() == 0
        || latestAspects.getValues() == null
        || latestAspects.getMetadata() == null) {
      log.debug("Found 0 formInfo aspects for forms. Skipping migration.");
      return 0;
    }

    if (latestAspects.getValues().size() != latestAspects.getMetadata().getExtraInfos().size()) {
      // Bad result -- we should log that we cannot migrate this batch of formInfos.
      log.warn(
          "Failed to match formInfo aspects with corresponding urns. Found mismatched length between aspects ({})"
              + "and metadata ({}) for metadata {}",
          latestAspects.getValues().size(),
          latestAspects.getMetadata().getExtraInfos().size(),
          latestAspects.getMetadata());
      return latestAspects.getTotalCount();
    }

    List<Future<?>> futures = new LinkedList<>();
    for (int i = 0; i < latestAspects.getValues().size(); i++) {
      ExtraInfo info = latestAspects.getMetadata().getExtraInfos().get(i);
      RecordTemplate formInfoRecord = latestAspects.getValues().get(i);
      Urn urn = info.getUrn();
      FormInfo formInfo = (FormInfo) formInfoRecord;
      if (formInfo == null) {
        log.warn("Received null formInfo for urn {}", urn);
        continue;
      }

      futures.add(
          entityService
              .alwaysProduceMCLAsync(
                  systemOperationContext,
                  urn,
                  Constants.FORM_ENTITY_NAME,
                  Constants.FORM_INFO_ASPECT_NAME,
                  formInfoAspectSpec,
                  null,
                  formInfo,
                  null,
                  null,
                  auditStamp,
                  ChangeType.RESTATE)
              .getFirst());
    }

    futures.stream()
        .filter(Objects::nonNull)
        .forEach(
            f -> {
              try {
                f.get();
              } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
              }
            });

    return latestAspects.getTotalCount();
  }
}
