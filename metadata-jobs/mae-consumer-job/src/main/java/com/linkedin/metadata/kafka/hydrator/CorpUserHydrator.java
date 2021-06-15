package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CorpUserHydrator extends BaseHydrator<CorpUserSnapshot> {

  private static final String USER_NAME = "username";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromSnapshot(ObjectNode document, CorpUserSnapshot snapshot) {
    for (CorpUserAspect aspect : snapshot.getAspects()) {
      if (aspect.isCorpUserInfo()) {
        document.put(NAME, aspect.getCorpUserInfo().getDisplayName());
      } else if (aspect.isCorpUserKey()) {
        document.put(USER_NAME, aspect.getCorpUserKey().getUsername());
      }
    }
  }
}
