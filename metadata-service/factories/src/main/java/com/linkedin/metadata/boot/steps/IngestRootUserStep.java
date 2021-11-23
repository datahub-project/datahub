package com.linkedin.metadata.boot.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;

import java.io.IOException;
import java.net.URISyntaxException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;


@Slf4j
@RequiredArgsConstructor
public class IngestRootUserStep implements BootstrapStep {

  private static final String USER_INFO_ASPECT_NAME = "corpUserInfo";

  private final EntityService _entityService;

  @Override
  public String name() {
    return getClass().getSimpleName();
  }

  @Override
  public void execute() throws IOException, URISyntaxException {

    final ObjectMapper mapper = new ObjectMapper();

    // 1. Read from the file into JSON.
    final JsonNode userObj = mapper.readTree(new ClassPathResource("./boot/root_user.json").getFile());

    if (!userObj.isObject()) {
      throw new RuntimeException(String.format("Found malformed root user file, expected an Object but found %s",
          userObj.getNodeType()));
    }

    // 2. Ingest the user info
    final Urn urn;
    try {
      urn = Urn.createFromString(userObj.get("urn").asText());
    } catch (URISyntaxException e) {
      log.error("Malformed urn: {}", userObj.get("urn").asText());
      throw new RuntimeException("Malformed urn", e);
    }

    final CorpUserInfo info =
        RecordUtils.toRecordTemplate(CorpUserInfo.class, userObj.get("info").toString());
    final AuditStamp aspectAuditStamp =
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());
    _entityService.ingestAspect(urn, USER_INFO_ASPECT_NAME, info, aspectAuditStamp);
  }
}