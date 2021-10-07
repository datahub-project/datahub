package com.linkedin.metadata.boot.steps;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.DataPlatformInstanceUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import io.ebean.EbeanServer;
import io.ebean.PagedList;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class IngestDataPlatformInstancesStep implements BootstrapStep {
  private static final String PLATFORM_INSTANCE_ASPECT_NAME = "dataPlatformInstance";
  private static final int BATCH_SIZE = 1000;

  private final EntityService _entityService;
  private final EbeanServer _server;

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  private PagedList<EbeanAspectV2> getPagedAspects(final int start, final int pageSize) {
    return _server.find(EbeanAspectV2.class)
        .setDistinct(true)
        .select(EbeanAspectV2.URN_COLUMN)
        .orderBy()
        .asc(EbeanAspectV2.URN_COLUMN)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .findPagedList();
  }

  private Optional<DataPlatformInstance> getDataPlatformInstance(Urn urn) {
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(urn);
    RecordTemplate keyAspect = EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec.getPegasusSchema());
    return DataPlatformInstanceUtils.buildDataPlatformInstance(urn.getEntityType(), keyAspect);
  }

  @Override
  public void execute() throws Exception {
    log.info("Checking for DataPlatformInstance");
    boolean hasDataPlatformInstance = _server.find(EbeanAspectV2.class)
        .where()
        .eq(EbeanAspectV2.ASPECT_COLUMN, PLATFORM_INSTANCE_ASPECT_NAME)
        .exists();
    if (hasDataPlatformInstance) {
      log.info("DataPlaformInstance aspect exists. Skipping step");
      return;
    }

    int numEntities = _server.find(EbeanAspectV2.class).setDistinct(true).select(EbeanAspectV2.URN_COLUMN).findCount();
    int start = 0;

    while (start < numEntities) {
      log.info("Reading urns {} to {} from the aspects table to generate dataplatform instance aspects", start,
          start + BATCH_SIZE);
      PagedList<EbeanAspectV2> aspects = getPagedAspects(start, start + BATCH_SIZE);
      for (EbeanAspectV2 aspect : aspects.getList()) {
        Urn urn = Urn.createFromString(aspect.getUrn());
        Optional<DataPlatformInstance> dataPlatformInstance = getDataPlatformInstance(urn);
        if (!dataPlatformInstance.isPresent()) {
          continue;
        }

        final AuditStamp aspectAuditStamp =
            new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());

        _entityService.ingestAspect(urn, PLATFORM_INSTANCE_ASPECT_NAME, dataPlatformInstance.get(), aspectAuditStamp);
      }
      log.info("Finished ingesting DataPlaformInstance for urn {} to {}", start, start + BATCH_SIZE);
      start += BATCH_SIZE;
    }
    log.info("Finished ingesting DataPlaformInstance for all entities");
  }
}
