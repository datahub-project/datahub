package com.linkedin.metadata.boot.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.HashMap;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class RemoveUnknownAspectsStep implements BootstrapStep {

  private final EntityRegistry _entityRegistry;
  private final EntityService _entityService;
  private final AspectMigrationsDao _migrationsDao;

  private static final int BATCH_SIZE = 1000;

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void execute() throws Exception {

    long numEntities = _migrationsDao.countAspects();
    int start = 0;

    while (start < numEntities) {
      log.info("Reading urns {} to {} from the aspects table to remove unknown aspects.", start,
          start + BATCH_SIZE);
      Iterable<EntityAspectIdentifier> identifiers = _migrationsDao.listAllPrimaryKeys(start, start + BATCH_SIZE);
      for (EntityAspectIdentifier identifier : identifiers) {
        Urn urn = UrnUtils.getUrn(identifier.getUrn());

        if (!_entityRegistry.getEntitySpec(urn.getEntityType()).hasAspect(identifier.getAspect())) {
          log.info("Deleting unknown aspect {} for urn {}", identifier.getAspect(), identifier.getUrn());
          _entityService.deleteAspect(identifier.getUrn(), identifier.getAspect(), new HashMap<>(), true);
        }
      }
      log.info("Finished ingesting DataPlatformInstance for urn {} to {}", start, start + BATCH_SIZE);
      start += BATCH_SIZE;
    }
    log.info("Finished ingesting DataPlatformInstance for all entities");
  }
}
