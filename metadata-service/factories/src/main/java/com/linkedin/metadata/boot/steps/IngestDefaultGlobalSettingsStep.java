package com.linkedin.metadata.boot.steps;

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
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.GlobalSettingsInfo;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

/**
 * This bootstrap step is responsible for ingesting a default Global Settings object if it does not
 * already exist.
 *
 * <p>If settings already exist, we merge the defaults and the existing settings such that the
 * container will also get new settings when they are added.
 */
@Slf4j
public class IngestDefaultGlobalSettingsStep implements BootstrapStep {

  private static final String DEFAULT_SETTINGS_RESOURCE_PATH = "./boot/global_settings.json";
  private final EntityService<?> _entityService;
  private final String _resourcePath;

  public IngestDefaultGlobalSettingsStep(@Nonnull final EntityService<?> entityService) {
    this(entityService, DEFAULT_SETTINGS_RESOURCE_PATH);
  }

  public IngestDefaultGlobalSettingsStep(
      @Nonnull final EntityService<?> entityService, @Nonnull final String resourcePath) {
    _entityService = Objects.requireNonNull(entityService);
    _resourcePath = Objects.requireNonNull(resourcePath);
  }

  @Override
  public String name() {
    return getClass().getName();
  }

  @Override
  public void execute(@Nonnull OperationContext systemOperationContext)
      throws IOException, URISyntaxException {

    final ObjectMapper mapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    mapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());

    log.info("Ingesting default global settings...");

    // 1. Read from the file into JSON.
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

    // 2. Bind the global settings json into a GlobalSettingsInfo aspect.
    GlobalSettingsInfo defaultSettings;
    defaultSettings =
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

    // 3. Get existing settings or empty settings object
    final GlobalSettingsInfo existingSettings =
        getExistingGlobalSettingsOrEmpty(systemOperationContext);

    // 4. Merge existing settings onto previous settings. Be careful - if we change the settings
    // schema dramatically in future we may need to account for that.
    final GlobalSettingsInfo newSettings =
        new GlobalSettingsInfo(mergeDataMaps(defaultSettings.data(), existingSettings.data()));

    // 5. Ingest into DataHub.
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
  }

  private GlobalSettingsInfo getExistingGlobalSettingsOrEmpty(
      @Nonnull OperationContext systemOperationContext) {
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
