package com.linkedin.metadata.boot.steps;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.linkedin.metadata.entity.ebean.transactions.AspectsBatch;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;


@Slf4j
@RequiredArgsConstructor
public class IngestDataPlatformsStep implements BootstrapStep {

  private static final String PLATFORM_ASPECT_NAME = "dataPlatformInfo";

  private final EntityService _entityService;

  @Override
  public String name() {
    return "IngestDataPlatformsStep";
  }

  @Override
  public void execute() throws IOException, URISyntaxException {

    final ObjectMapper mapper = new ObjectMapper();

    // 1. Read from the file into JSON.
    final JsonNode dataPlatforms = mapper.readTree(new ClassPathResource("./boot/data_platforms.json").getFile());

    if (!dataPlatforms.isArray()) {
      throw new RuntimeException(String.format("Found malformed data platforms file, expected an Array but found %s",
          dataPlatforms.getNodeType()));
    }

    // 2. For each JSON object, cast into a DataPlatformSnapshot object.
    List<AspectsBatchItem> dataPlatformAspects =  StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(dataPlatforms.iterator(), Spliterator.ORDERED), false)
            .map(dataPlatform -> {
              final String urnString;
              final Urn urn;
              try {
                urnString = dataPlatform.get("urn").asText();
                urn = Urn.createFromString(urnString);
              } catch (URISyntaxException e) {
                log.error("Malformed urn: {}", dataPlatform.get("urn").asText());
                throw new RuntimeException("Malformed urn", e);
              }

              final DataPlatformInfo info =
                      RecordUtils.toRecordTemplate(DataPlatformInfo.class, dataPlatform.get("aspect").toString());

              return AspectsBatchItem.builder()
                      .urn(urn)
                      .aspectName(PLATFORM_ASPECT_NAME)
                      .value(info)
                      .build(_entityService.getEntityRegistry());
            }).collect(Collectors.toList());

    _entityService.ingestAspects(AspectsBatch.builder().items(dataPlatformAspects).build(),
            new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis()),
            true,
            false);
  }
}
