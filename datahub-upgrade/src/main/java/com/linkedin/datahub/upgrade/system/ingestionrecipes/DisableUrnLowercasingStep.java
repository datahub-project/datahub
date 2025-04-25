package com.linkedin.datahub.upgrade.system.ingestionrecipes;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DisableUrnLowercasingStep implements UpgradeStep {

  public static final String RECIPE_SOURCE_NODE_NAME = "source";
  public static final String RECIPE_CONFIG_NODE_NAME = "config";
  public static final String CONVERT_URNS_TO_LOWERCASE_CONFIG_NAME = "convert_urns_to_lowercase";
  private static final String UPGRADE_ID = "DisableUrnLowercasingStep";
  public static final String DATAHUB_CLI_EXECUTOR_ID = "__datahub_cli_";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);
  public static final List<String> SUPPORTED_TYPES =
      Collections.unmodifiableList(
          Arrays.asList(
              "athena",
              "bigquery",
              "clickhouse",
              "cockroachdb",
              "druid",
              "hana",
              "hive",
              "hive_metastore",
              "mariadb",
              "mysql",
              "oracle",
              "postgres",
              "presto",
              "presto",
              "redshift",
              "teradata",
              "trino",
              "vertica"));
  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final ObjectMapper mapper = new ObjectMapper();

  public DisableUrnLowercasingStep(OperationContext opContext, EntityService<?> entityService) {
    this.opContext = opContext;
    this.entityService = entityService;
  }

  @Override
  public String id() {
    return "ingestion-recipe-urn-lowercasing-v1";
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      int start = 0;
      int count = 100;
      boolean hasMore = true;

      while (hasMore) {
        ListResult<RecordTemplate> entities =
            this.entityService.listLatestAspects(
                this.opContext,
                INGESTION_SOURCE_ENTITY_NAME,
                INGESTION_INFO_ASPECT_NAME,
                start,
                count);
        if (entities.getValues().size() != entities.getMetadata().getExtraInfos().size()) {
          // Bad result -- we should log that we cannot migrate this batch of formInfos.
          log.warn(
              "Failed to match formInfo aspects with corresponding urns. Found mismatched length between aspects ({})"
                  + "and metadata ({}) for metadata {}",
              entities.getValues().size(),
              entities.getMetadata().getExtraInfos().size(),
              entities.getMetadata());
          throw new RuntimeException("Failed to match formInfo aspects with corresponding urns");
        }

        int num = 0;
        for (RecordTemplate entity : entities.getValues()) {
          DataHubIngestionSourceInfo sourceInfo = (DataHubIngestionSourceInfo) entity;
          if (!SUPPORTED_TYPES.contains(sourceInfo.getType())
              || DATAHUB_CLI_EXECUTOR_ID.equals(sourceInfo.getConfig().getExecutorId())) {
            continue;
          }
          sourceInfo.getConfig().getRecipe();
          try {
            if (updateRecipe(sourceInfo)) {
              updateSourceInfo(
                  sourceInfo, entities.getMetadata().getExtraInfos().get(num).getUrn());
            }
            num++;
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        }
        if (entities.getValues().size() < count) {
          hasMore = false;
        } else {
          start += count;
        }
      }
      BootstrapStep.setUpgradeResult(context.opContext(), UPGRADE_ID_URN, entityService);
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private boolean updateRecipe(DataHubIngestionSourceInfo sourceInfo)
      throws JsonProcessingException {
    if ((sourceInfo != null)) {
      String jsonRecipe = sourceInfo.getConfig().getRecipe();
      JsonNode rootNode = mapper.readTree(jsonRecipe);
      JsonNode sourceNode = rootNode.path(RECIPE_SOURCE_NODE_NAME);
      if (sourceNode.isObject()) {
        JsonNode configNode = sourceNode.path(RECIPE_CONFIG_NODE_NAME);
        if (configNode.isObject()) {
          if (configNode.get(CONVERT_URNS_TO_LOWERCASE_CONFIG_NAME) == null) {
            ((ObjectNode) configNode).put(CONVERT_URNS_TO_LOWERCASE_CONFIG_NAME, false);
            sourceInfo.getConfig().setRecipe(mapper.writeValueAsString(rootNode));
            return true;
          }
        }
      }
    }
    return false;
  }

  private void updateSourceInfo(DataHubIngestionSourceInfo sourceInfo, Urn urn) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(Constants.INGESTION_SOURCE_ENTITY_NAME);
    proposal.setAspectName(Constants.INGESTION_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(sourceInfo));
    proposal.setChangeType(ChangeType.UPSERT);
    log.info("About to ingest datahub ingetion source metadata {}", proposal);
    final AuditStamp auditStamp = opContext.getAuditStamp();
    this.entityService.ingestProposal(this.opContext, proposal, auditStamp, false);
  }
}
