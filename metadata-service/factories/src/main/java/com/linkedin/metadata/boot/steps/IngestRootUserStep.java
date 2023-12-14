package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
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
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    mapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());

    // 1. Read from the file into JSON.
    final JsonNode userObj =
        mapper.readTree(new ClassPathResource("./boot/root_user.json").getFile());

    if (!userObj.isObject()) {
      throw new RuntimeException(
          String.format(
              "Found malformed root user file, expected an Object but found %s",
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
    final CorpUserKey key =
        (CorpUserKey) EntityKeyUtils.convertUrnToEntityKey(urn, getUserKeyAspectSpec());
    final AuditStamp aspectAuditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    _entityService.ingestAspects(
        urn,
        List.of(Pair.of(CORP_USER_KEY_ASPECT_NAME, key), Pair.of(USER_INFO_ASPECT_NAME, info)),
        aspectAuditStamp,
        null);
  }

  private AspectSpec getUserKeyAspectSpec() {
    final EntitySpec spec = _entityService.getEntityRegistry().getEntitySpec(CORP_USER_ENTITY_NAME);
    return spec.getKeyAspectSpec();
  }
}
