package com.linkedin.datahub.upgrade.system.restoreindices.forminfo;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreFormInfoIndicesStep implements UpgradeStep {

  private static final String STEP_ID = "restore-form-info-indices";
  private static final Integer BATCH_SIZE = 1000;

  private final EntityService<?> _entityService;

  public RestoreFormInfoIndicesStep(@Nonnull final EntityService<?> entityService) {
    _entityService = entityService;
  }

  @Override
  public String id() {
    return STEP_ID;
  }

  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  public boolean skip(final UpgradeContext context) {
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        execute(context.opContext());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Failed to restore form info indices", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void execute(@Nonnull final OperationContext systemOperationContext) throws Exception {
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

  private int getAndRestoreFormInfoIndices(
      @Nonnull OperationContext systemOperationContext, int start, AuditStamp auditStamp) {
    final AspectSpec formInfoAspectSpec =
        systemOperationContext
            .getEntityRegistry()
            .getEntitySpec(Constants.FORM_ENTITY_NAME)
            .getAspectSpec(Constants.FORM_INFO_ASPECT_NAME);

    final ListResult<RecordTemplate> latestAspects =
        _entityService.listLatestAspects(
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
          _entityService
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
