package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.models.EntityKeyUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import java.util.Optional;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@SuperBuilder
public abstract class Hydrator<KEY_ASPECT extends RecordTemplate> {
  private final EntityClient entityClient;
  private final EntityRegistry entityRegistry;

  /**
   * Use values in the KeyAspect to hydrate the document
   */
  protected abstract void hydrateFromKey(ObjectNode document, KEY_ASPECT key);

  /**
   * Use values in the snapshot to hydrate the document
   */
  protected abstract void hydrateFromSnapshot(ObjectNode document, Snapshot snapshot);

  public Optional<ObjectNode> getHydratedEntity(String urn) {
    final ObjectNode document = JsonNodeFactory.instance.objectNode();
    // Hydrate fields from urn
    Urn urnObj;
    try {
      urnObj = Urn.createFromString(urn);
    } catch (URISyntaxException e) {
      log.info("Invalid DataJob URN: {}", urn);
      return Optional.empty();
    }
    final RecordTemplate keyAspect = EntityKeyUtils.convertUrnToEntityKey(urnObj, entityRegistry);
    try {
      hydrateFromKey(document, (KEY_ASPECT) keyAspect);
    } catch (ClassCastException exception) {
      log.error("{}: input urn {} does not match KeyAspect", this.getClass().getSimpleName(), urn);
    }
    // Hydrate fields from snapshot
    Entity entity;
    try {
      entity = entityClient.get(urnObj);
    } catch (RemoteInvocationException e) {
      log.error("Error while calling GMS to hydrate entity for urn {}", urn);
      e.printStackTrace();
      return Optional.of(document);
    }
    hydrateFromSnapshot(document, entity.getValue());
    return Optional.of(document);
  }
}
