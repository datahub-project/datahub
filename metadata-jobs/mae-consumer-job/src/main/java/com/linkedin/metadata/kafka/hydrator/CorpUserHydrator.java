package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.snapshot.Snapshot;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@SuperBuilder
public class CorpUserHydrator extends Hydrator<CorpUserKey> {

  private static final String USER_NAME = "username";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromKey(ObjectNode document, CorpUserKey key) {
    document.put(USER_NAME, key.getUsername());
  }

  @Override
  protected void hydrateFromSnapshot(ObjectNode document, Snapshot snapshot) {
    if (!snapshot.isCorpUserSnapshot()) {
      log.error("Hydrator {} does not match type of snapshot {}", this.getClass().getSimpleName(),
          snapshot.getClass().getSimpleName());
    }
    for (CorpUserAspect aspect : snapshot.getCorpUserSnapshot().getAspects()) {
      if (aspect.isCorpUserInfo()) {
        document.put(NAME, aspect.getCorpUserInfo().getDisplayName());
      }
    }
  }
}
