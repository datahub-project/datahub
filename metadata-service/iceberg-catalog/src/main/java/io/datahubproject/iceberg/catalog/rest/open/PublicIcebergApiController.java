package io.datahubproject.iceberg.catalog.rest.open;

import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static io.datahubproject.iceberg.catalog.Utils.*;

import com.google.common.base.Strings;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.TagUrn;
import io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse;
import io.datahubproject.iceberg.catalog.rest.secure.AbstractIcebergController;
import java.net.URISyntaxException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/public-iceberg")
public class PublicIcebergApiController extends AbstractIcebergController {

  @Value("${icebergCatalog.enablePublicRead}")
  private boolean isPublicReadEnabled;

  @Value("${icebergCatalog.publiclyReadableTag}")
  private String publiclyReadableTag;

  private static final String ACCESS_TYPE_KEY = "access-type";
  private static final String ACCESS_TYPE_PUBLIC_READ = "PUBLIC_READ";

  @GetMapping(value = "/v1/config", produces = MediaType.APPLICATION_JSON_VALUE)
  public ConfigResponse getConfig(
      @RequestParam(value = "warehouse", required = true) String warehouse) {
    log.info("GET CONFIG for warehouse {}", warehouse);

    checkPublicEnabled();

    // check that warehouse exists
    warehouse(warehouse, systemOperationContext);
    ConfigResponse response = ConfigResponse.builder().withOverride("prefix", warehouse).build();
    log.info("GET CONFIG response: {}", response);
    return response;
  }

  @GetMapping(
      value = "/v1/{prefix}/namespaces/{namespace}/tables/{table}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public LoadTableResponse loadTable(
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @PathVariable("table") String table,
      @RequestHeader(value = "X-Iceberg-Access-Delegation", required = false)
          String xIcebergAccessDelegation,
      @RequestParam(value = "snapshots", required = false) String snapshots) {
    log.info("GET TABLE REQUEST {}.{}.{}", platformInstance, namespace, table);

    checkPublicEnabled();

    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, systemOperationContext);
    Optional<DatasetUrn> datasetUrn = warehouse.getDatasetUrn(tableIdFromString(namespace, table));
    if (datasetUrn.isPresent()) {
      GlobalTags tags =
          (GlobalTags)
              entityService.getLatestAspect(
                  systemOperationContext, datasetUrn.get(), GLOBAL_TAGS_ASPECT_NAME);
      if (tags != null && tags.hasTags()) {
        for (TagAssociation tag : tags.getTags()) {
          if (publicTag().equals(tag.getTag())) {
            LoadTableResponse getTableResponse =
                catalogOperation(
                    platformInstance,
                    catalog ->
                        CatalogHandlers.loadTable(catalog, tableIdFromString(namespace, table)));

            log.info("GET TABLE RESPONSE {}", getTableResponse);
            return getTableResponse;
          }
        }
      }
    }

    throw new NoSuchTableException(
        "No such table %s", fullTableName(platformInstance, tableIdFromString(namespace, table)));
  }

  void checkPublicEnabled() {
    if (!isPublicReadEnabled || Strings.isNullOrEmpty(publiclyReadableTag)) {
      throw new NotFoundException("No endpoint GET /v1/config");
    }
  }

  TagUrn publicTag() {
    try {
      return TagUrn.createFromString("urn:li:tag:" + publiclyReadableTag);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid public tag " + publiclyReadableTag, e);
    }
  }
}
