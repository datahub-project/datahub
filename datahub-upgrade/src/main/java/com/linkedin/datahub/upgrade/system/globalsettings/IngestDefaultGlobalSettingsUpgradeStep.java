package com.linkedin.datahub.upgrade.system.globalsettings;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.validation.CoercionMode;
import com.linkedin.data.schema.validation.RequiredMode;
import com.linkedin.data.schema.validation.UnrecognizedFieldMode;
import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

/**
 * System-update blocking step that ingests default global settings on every deploy, merging with
 * any existing settings so new defaults are automatically picked up.
 */
@Slf4j
public class IngestDefaultGlobalSettingsUpgradeStep implements UpgradeStep {

  private static final String STEP_ID = "ingest-default-global-settings";
  private static final String DEFAULT_SETTINGS_RESOURCE_PATH = "./boot/global_settings.json";

  private final EntityService<?> _entityService;
  private final boolean _enabled;
  private final String _resourcePath;

  public IngestDefaultGlobalSettingsUpgradeStep(
      @Nonnull final EntityService<?> entityService, final boolean enabled) {
    this(entityService, enabled, DEFAULT_SETTINGS_RESOURCE_PATH);
  }

  // Package-private: used in tests
  IngestDefaultGlobalSettingsUpgradeStep(
      @Nonnull final EntityService<?> entityService,
      final boolean enabled,
      @Nonnull final String resourcePath) {
    _entityService = entityService;
    _enabled = enabled;
    _resourcePath = resourcePath;
  }

  @Override
  public String id() {
    return STEP_ID;
  }

  @Override
  public boolean isOptional() {
    return false;
  }

  @Override
  public boolean skip(final UpgradeContext context) {
    if (!_enabled) {
      log.info("Ingest default global settings step is disabled; skipping.");
      return true;
    }
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        execute(context.opContext());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Failed to ingest default global settings", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void execute(@Nonnull final OperationContext systemOperationContext) throws Exception {
    final ObjectMapper mapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    mapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());

    log.info("Ingesting default global settings...");

    JsonNode defaultSettingsObj;
    try {
      defaultSettingsObj = mapper.readTree(new ClassPathResource(_resourcePath).getInputStream());
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to parse global settings file. Could not parse valid json at resource path %s",
              _resourcePath),
          e);
    }

    if (!defaultSettingsObj.isObject()) {
      throw new RuntimeException(
          String.format(
              "Found malformed global settings info file, expected an Object but found %s",
              defaultSettingsObj.getNodeType()));
    }

    GlobalSettingsInfo defaultSettings =
        RecordUtils.toRecordTemplate(GlobalSettingsInfo.class, defaultSettingsObj.toString());
    ValidationResult result =
        ValidateDataAgainstSchema.validate(
            defaultSettings,
            new ValidationOptions(
                RequiredMode.CAN_BE_ABSENT_IF_HAS_DEFAULT,
                CoercionMode.NORMAL,
                UnrecognizedFieldMode.DISALLOW));

    if (!result.isValid()) {
      throw new RuntimeException(
          String.format(
              "Failed to parse global settings file. Provided JSON does not match GlobalSettingsInfo.pdl model. %s",
              result.getMessages()));
    }

    final GlobalSettingsInfo existingSettings =
        getExistingGlobalSettingsOrEmpty(systemOperationContext);

    final GlobalSettingsInfo newSettings =
        new GlobalSettingsInfo(mergeDataMaps(defaultSettings.data(), existingSettings.data()));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(GLOBAL_SETTINGS_URN);
    proposal.setEntityType(GLOBAL_SETTINGS_ENTITY_NAME);
    proposal.setAspectName(GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newSettings));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(
        systemOperationContext,
        proposal,
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis()),
        false);

    log.info("Successfully ingested default global settings.");
  }

  private GlobalSettingsInfo getExistingGlobalSettingsOrEmpty(
      @Nonnull final OperationContext systemOperationContext) {
    RecordTemplate aspect =
        _entityService.getAspect(
            systemOperationContext, GLOBAL_SETTINGS_URN, GLOBAL_SETTINGS_INFO_ASPECT_NAME, 0);
    return aspect != null ? (GlobalSettingsInfo) aspect : new GlobalSettingsInfo();
  }

  private DataMap mergeDataMaps(final DataMap map1, final DataMap map2) {
    final DataMap result = new DataMap();
    // TODO: Replace with a nested merge. This only copies top level keys.
    result.putAll(map1);
    result.putAll(map2);
    return result;
  }
}
