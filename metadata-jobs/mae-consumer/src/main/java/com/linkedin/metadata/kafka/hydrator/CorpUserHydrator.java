package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.dao.RestliRemoteDAO;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.restli.client.Client;
import java.net.URISyntaxException;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CorpUserHydrator implements Hydrator {
  private final Client _restliClient;
  private final RestliRemoteDAO<CorpUserSnapshot, CorpUserAspect, CorpuserUrn> _remoteDAO;

  private static final String USER_NAME = "username";
  private static final String NAME = "name";

  public CorpUserHydrator(@Nonnull Client restliClient) {
    _restliClient = restliClient;
    _remoteDAO = new RestliRemoteDAO<>(CorpUserSnapshot.class, CorpUserAspect.class, _restliClient);
  }

  @Override
  public Optional<ObjectNode> getHydratedEntity(String urn) {
    CorpuserUrn corpuserUrn;
    try {
      corpuserUrn = CorpuserUrn.createFromString(urn);
    } catch (URISyntaxException e) {
      log.info("Invalid CorpUser URN: {}", urn);
      return Optional.empty();
    }

    ObjectNode jsonObject = HydratorFactory.OBJECT_MAPPER.createObjectNode();
    jsonObject.put(USER_NAME, corpuserUrn.getUsernameEntity());

    _remoteDAO.get(CorpUserInfo.class, corpuserUrn)
        .ifPresent(corpUserInfo -> jsonObject.put(NAME, corpUserInfo.getDisplayName()));

    return Optional.of(jsonObject);
  }
}
