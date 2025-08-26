package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.datahub.util.exception.ModelConversionException;
import com.google.common.collect.Streams;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.transform.filter.request.MaskTree;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.config.structuredProperties.extensions.AspectConfiguration;
import com.linkedin.metadata.config.structuredProperties.extensions.EntityConfiguration;
import com.linkedin.metadata.config.structuredProperties.extensions.ExtendedModelValidationConfiguration;
import com.linkedin.metadata.config.structuredProperties.extensions.FieldConfiguration;
import com.linkedin.metadata.config.structuredProperties.extensions.StructuredPropertyConfiguration;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.restli.internal.server.util.RestUtils;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.PropertyValueArray;
import com.linkedin.structured.StructuredPropertyDefinition;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

/**
 * This mutator will convert specified fields within aspects that are unrecognized to structured
 * properties
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class ExtendedModelStructuredPropertyMutator extends MutationHook {
  @Nonnull private AspectPluginConfig config;

  @Nonnull
  private Map<AspectPluginConfig.EntityAspectName, Map<String, StructuredPropertyDefinition>>
      aspectFieldMappings;

  @Nonnull private Map<String, StructuredPropertyDefinition> structuredPropertyMappings;

  private static String DATA_TYPE_URN_PREFIX = "urn:li:dataType:datahub.";
  private static String ENTITY_TYPE_URN_PREFIX = "urn:li:entityType:datahub.";
  private static String STRUCTURED_PROPERTY_FIELD = "structuredProperty";

  private static Urn STRING_DATA_TYPE_URN = UrnUtils.getUrn("urn:li:dataType:datahub.string");
  private static Urn NUMBER_DATA_TYPE_URN = UrnUtils.getUrn("urn:li:dataType:datahub.number");
  private static Urn URN_DATA_TYPE_URN = UrnUtils.getUrn("urn:li:dataType:datahub.urn");

  private static String EDITABLE_SCHEMA_FIELD_INFO = "editableSchemaFieldInfo";
  private static String SCHEMA_FIELDS = "fields";
  private static String SCHEMA_FIELD_PATH = "fieldPath";

  public ExtendedModelStructuredPropertyMutator(
      ExtendedModelValidationConfiguration config, boolean extensionsEnabled) {
    List<AspectPluginConfig.EntityAspectName> entityAspectNames = new ArrayList<>();
    Map<String, StructuredPropertyDefinition> structuredPropertyMappings =
        createStructuredPropertyMappings(config);
    Map<AspectPluginConfig.EntityAspectName, Map<String, StructuredPropertyDefinition>>
        aspectFieldMappings =
            createAspectFieldMappings(structuredPropertyMappings, config, entityAspectNames);

    setConfig(
            AspectPluginConfig.builder()
                .className(ExtendedModelStructuredPropertyMutator.class.getName())
                .enabled(extensionsEnabled)
                .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT"))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName("*")
                            .aspectName("*")
                            .build()))
                .build())
        .setAspectFieldMappings(aspectFieldMappings)
        .setStructuredPropertyMappings(structuredPropertyMappings);
  }

  private Map<String, StructuredPropertyDefinition> createStructuredPropertyMappings(
      ExtendedModelValidationConfiguration config) {
    Map<String, StructuredPropertyDefinition> structuredPropertyMappings = new HashMap<>();

    for (StructuredPropertyConfiguration structuredPropertyConfiguration :
        config.getStructuredProperties()) {
      StructuredPropertyDefinition structuredPropertyDefinition =
          new StructuredPropertyDefinition();
      if (StringUtils.isBlank(structuredPropertyConfiguration.getQualifiedName())) {
        log.error(
            "Unable to parse configured structured property extension due to invalid qualified name: {}",
            structuredPropertyConfiguration);
        continue;
      }
      structuredPropertyDefinition.setQualifiedName(
          structuredPropertyConfiguration.getQualifiedName());

      if (StringUtils.isBlank(structuredPropertyConfiguration.getType())) {
        log.error(
            "Unable to parse configured structured property extension due to invalid type: {}",
            structuredPropertyConfiguration);
        continue;
      }
      structuredPropertyDefinition.setValueType(
          UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + structuredPropertyConfiguration.getType()));

      structuredPropertyDefinition.setCardinality(
          structuredPropertyConfiguration.getCardinality() != null
              ? mapCardinality(structuredPropertyConfiguration.getCardinality())
              : PropertyCardinality.SINGLE);
      if (structuredPropertyConfiguration.getDescription() != null) {
        structuredPropertyDefinition.setDescription(
            structuredPropertyConfiguration.getDescription());
      }
      if (structuredPropertyConfiguration.getDisplayName() != null) {
        structuredPropertyDefinition.setDisplayName(
            structuredPropertyConfiguration.getDisplayName());
      }
      if (structuredPropertyConfiguration.getAllowedValues() != null) {
        structuredPropertyDefinition.setAllowedValues(
            mapAllowedValues(structuredPropertyConfiguration.getAllowedValues()));
      }

      // Required to set entity types after this step
      structuredPropertyMappings.put(
          structuredPropertyConfiguration.getQualifiedName(), structuredPropertyDefinition);
    }

    return structuredPropertyMappings;
  }

  private Map<AspectPluginConfig.EntityAspectName, Map<String, StructuredPropertyDefinition>>
      createAspectFieldMappings(
          Map<String, StructuredPropertyDefinition> structuredPropertyMappings,
          ExtendedModelValidationConfiguration config,
          List<AspectPluginConfig.EntityAspectName> entityAspectNames) {
    Map<AspectPluginConfig.EntityAspectName, Map<String, StructuredPropertyDefinition>>
        aspectFieldMappings = new HashMap<>();
    for (EntityConfiguration entityConfiguration : config.getEntities()) {
      AspectPluginConfig.EntityAspectName.EntityAspectNameBuilder entityAspectNameBuilder =
          AspectPluginConfig.EntityAspectName.builder().entityName(entityConfiguration.getEntity());
      for (AspectConfiguration aspectConfiguration : entityConfiguration.getAspects()) {
        AspectPluginConfig.EntityAspectName entityAspectName =
            entityAspectNameBuilder.aspectName(aspectConfiguration.getAspect()).build();
        entityAspectNames.add(entityAspectName);
        for (FieldConfiguration fieldConfiguration : aspectConfiguration.getFields()) {
          if (!structuredPropertyMappings.containsKey(
              fieldConfiguration.getConfig().get(STRUCTURED_PROPERTY_FIELD))) {
            log.error(
                "Unable to find valid structured property definition for field: {}",
                fieldConfiguration);
            continue;
          }
          // Add in supported entity type
          StructuredPropertyDefinition propertyDefinition =
              structuredPropertyMappings.get(
                  fieldConfiguration.getConfig().get(STRUCTURED_PROPERTY_FIELD));
          UrnArray entityTypesSupported =
              Optional.ofNullable(propertyDefinition.getEntityTypes(GetMode.NULL))
                  .orElse(new UrnArray());
          Set<Urn> entityTypesSupportedSet = new HashSet<>(entityTypesSupported);
          entityTypesSupportedSet.add(
              UrnUtils.getUrn(ENTITY_TYPE_URN_PREFIX + entityConfiguration.getEntity()));
          if (EDITABLE_SCHEMA_FIELD_INFO.equals(aspectConfiguration.getAspect())
              || SCHEMA_METADATA_ASPECT_NAME.equals(aspectConfiguration.getAspect())) {
            entityTypesSupported.add(
                UrnUtils.getUrn(ENTITY_TYPE_URN_PREFIX + SCHEMA_FIELD_ENTITY_NAME));
          }
          propertyDefinition.setEntityTypes(new UrnArray(entityTypesSupportedSet));

          Map<String, StructuredPropertyDefinition> fieldMapping =
              aspectFieldMappings.getOrDefault(entityAspectName, new HashMap<>());
          fieldMapping.put(fieldConfiguration.getFieldName(), propertyDefinition);
          aspectFieldMappings.put(entityAspectName, fieldMapping);
        }
      }
    }
    return aspectFieldMappings;
  }

  private static PropertyCardinality mapCardinality(
      @Nonnull StructuredPropertyConfiguration.Cardinality cardinality) {
    switch (cardinality) {
      case MULTIPLE:
        return PropertyCardinality.MULTIPLE;
      case SINGLE:
      default:
        return PropertyCardinality.SINGLE;
    }
  }

  private static PropertyValueArray mapAllowedValues(
      List<StructuredPropertyConfiguration.PropertyValue> propertyValues) {
    List<PropertyValue> props =
        propertyValues.stream()
            .map(
                propertyValue ->
                    new PropertyValue()
                        .setValue(PrimitivePropertyValue.create(propertyValue.getValue())))
            .collect(Collectors.toList());
    return new PropertyValueArray(props);
  }

  @Override
  protected Stream<MCPItem> proposalMutation(
      @Nonnull Collection<MCPItem> mcpItems, @Nonnull RetrieverContext retrieverContext) {
    final List<MCPItem> additionalChanges = new ArrayList<>();
    List<MCPItem> filteredOriginalItems =
        mcpItems.stream()
            .peek(
                item -> {
                  try {
                    DataMap dataMap =
                        RecordUtils.toDataMap(
                            item.getMetadataChangeProposal()
                                .getAspect()
                                .getValue()
                                .asString(StandardCharsets.UTF_8));
                    String entityName = item.getMetadataChangeProposal().getEntityType();
                    String aspectName = item.getMetadataChangeProposal().getAspectName();
                    AspectPluginConfig.EntityAspectName entityAspectName =
                        new AspectPluginConfig.EntityAspectName(entityName, aspectName);
                    Map<String, StructuredPropertyDefinition> fieldMap =
                        this.aspectFieldMappings.get(entityAspectName);
                    if (fieldMap == null) {
                      // No mapping, ignore
                      return;
                    }
                    for (String fieldKey : fieldMap.keySet()) {
                      // Nested keys are represented by dots, assumes no field itself has a dot in
                      // it which should be safe
                      // due to model constraints
                      String[] fieldParts = fieldKey.split("\\.");
                      Object value = dataMap;
                      int index = 0;
                      createStructuredPropertiesSideEffects(
                          index,
                          fieldParts,
                          additionalChanges,
                          value,
                          fieldMap,
                          item,
                          fieldKey,
                          dataMap,
                          retrieverContext);
                    }
                  } catch (NumberFormatException e) {
                    throw e;
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            // Ignore unknown after creating necessary additional changes
            .peek(
                item -> {
                  try {
                    AspectSpec aspectSpec =
                        item.getEntitySpec().getAspectSpec(item.getAspectName());
                    GenericAspect aspect = item.getMetadataChangeProposal().getAspect();
                    RecordTemplate recordTemplate =
                        GenericRecordUtils.deserializeAspect(
                            aspect.getValue(), aspect.getContentType(), aspectSpec);
                    try {
                      ValidationApiUtils.validateOrThrow(recordTemplate);
                    } catch (ValidationException | ModelConversionException e) {
                      log.warn(
                          "Failed to validate aspect. Coercing aspect {} on entity {}",
                          item.getAspectName(),
                          item.getEntitySpec().getName());
                      RestUtils.trimRecordTemplate(recordTemplate, new MaskTree(), false);
                      item.getMetadataChangeProposal()
                          .setAspect(GenericRecordUtils.serializeAspect(recordTemplate));
                    }
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            // Post filter for anything still unresolved
            .filter(
                item -> {
                  if (item.getEntitySpec().getAspectSpec(item.getAspectName()) == null) {
                    log.warn(
                        "Dropping unknown aspect {} on entity {}",
                        item.getAspectName(),
                        item.getAspectSpec().getName());
                    return false;
                  }
                  if (!"application/json"
                      .equals(item.getMetadataChangeProposal().getAspect().getContentType())) {
                    log.warn(
                        "Dropping unknown content type {} for aspect {} on entity {}",
                        item.getMetadataChangeProposal().getAspect().getContentType(),
                        item.getAspectName(),
                        item.getEntitySpec().getName());
                    return false;
                  }
                  return true;
                })
            .collect(Collectors.toList());
    // Recollect into intermediate list instead of direct use of stream to avoid concurrent
    // modification
    return Streams.concat(filteredOriginalItems.stream(), additionalChanges.stream());
  }

  private void createStructuredPropertiesSideEffects(
      int index,
      String[] fieldParts,
      List<MCPItem> additionalChanges,
      Object value,
      Map<String, StructuredPropertyDefinition> fieldMap,
      MCPItem item,
      String fieldKey,
      DataMap dataMap,
      RetrieverContext retrieverContext) {
    // Special handling for schema fields, store field path in case of a nested custom field like on
    // a tag/term
    String schemaFieldPath = null;
    for (int i = index; i < fieldParts.length; i++) {
      if (value instanceof List) {
        final int currentIndex = i;
        // Split off into sub-values and kill execution for current top level value
        ((List) value)
            .forEach(
                listItem ->
                    createStructuredPropertiesSideEffects(
                        currentIndex,
                        fieldParts,
                        additionalChanges,
                        listItem,
                        fieldMap,
                        item,
                        fieldKey,
                        dataMap,
                        retrieverContext));
        return;
      }
      if (value instanceof DataMap) {
        if (isSchemaField(fieldParts, item) && ((DataMap) value).containsKey(SCHEMA_FIELD_PATH)) {
          schemaFieldPath = (String) ((DataMap) value).get(SCHEMA_FIELD_PATH);
        }
        value = ((DataMap) value).get(fieldParts[i]);
      }
      if (value == null) {
        // value doesn't exist in DataMap, no need to do any mapping
        break;
      }
    }
    if (value != null) {
      StructuredPropertyDefinition structuredPropertyDefinition = fieldMap.get(fieldKey);
      StructuredPropertiesPatchBuilder propertiesBuilder = new StructuredPropertiesPatchBuilder();
      // Handle schema field case
      Urn patchItemUrn = resolveUrn(item, schemaFieldPath);
      propertiesBuilder.urn(patchItemUrn);
      if (structuredPropertyDefinition.getValueType().equals(STRING_DATA_TYPE_URN)
          || structuredPropertyDefinition.getValueType().equals(URN_DATA_TYPE_URN)) {
        if (PropertyCardinality.SINGLE.equals(structuredPropertyDefinition.getCardinality())) {
          String valueString = value.toString();
          propertiesBuilder.setStringProperty(
              StructuredPropertyUtils.toURNFromFQN(structuredPropertyDefinition.getQualifiedName()),
              valueString);
        } else {
          List<String> valueStrings;
          if (value instanceof List) {
            valueStrings =
                ((List<?>) value).stream().map(obj -> obj.toString()).collect(Collectors.toList());
          } else {
            valueStrings = Collections.singletonList(value.toString());
          }
          propertiesBuilder.setStringProperty(
              StructuredPropertyUtils.toURNFromFQN(structuredPropertyDefinition.getQualifiedName()),
              valueStrings);
        }
      } else if (structuredPropertyDefinition.getValueType().equals(NUMBER_DATA_TYPE_URN)) {
        Integer intValue;
        if (value instanceof Integer) {
          intValue = (Integer) value;
        } else if (value instanceof String) {
          intValue = Integer.parseInt((String) value);
        } else {
          throw new NumberFormatException("Unable to parse integer: " + value);
        }
        propertiesBuilder.setNumberProperty(
            StructuredPropertyUtils.toURNFromFQN(structuredPropertyDefinition.getQualifiedName()),
            intValue);
      } else {
        log.error(
            "Unsupported value type: {} unable to create structured property.",
            structuredPropertyDefinition.getValueType());
        return;
      }
      MetadataChangeProposal proposal = propertiesBuilder.build();
      EntitySpec entitySpec =
          retrieverContext
              .getAspectRetriever()
              .getEntityRegistry()
              .getEntitySpec(patchItemUrn.getEntityType());
      MCPItem patchItem =
          PatchItemImpl.builder()
              .build(
                  proposal,
                  item.getAuditStamp(),
                  retrieverContext.getAspectRetriever().getEntityRegistry());
      additionalChanges.add(patchItem);
    }
  }

  /**
   * Only SchemaFields are handled in a special way, if there are other virtual entities that need
   * to be specially handled those will need to be implemented later
   */
  private static Urn resolveUrn(MCPItem item, String schemaFieldPath) {
    // Special case for schema fields, apply to appropriate schema field based on aspect
    if (schemaFieldPath != null) {
      return getSchemaFieldUrn(item, schemaFieldPath);
    }

    // Otherwise apply to same urn as initial MCP
    return item.getUrn();
  }

  private static boolean isSchemaField(String[] fieldParts, MCPItem item) {
    return (EDITABLE_SCHEMA_METADATA_ASPECT_NAME.equals(item.getAspectName())
                || SCHEMA_METADATA_ASPECT_NAME.equals(item.getAspectName()))
            && EDITABLE_SCHEMA_FIELD_INFO.equals(fieldParts[0])
        || SCHEMA_FIELDS.equals(fieldParts[0]);
  }

  private static Urn getSchemaFieldUrn(MCPItem item, String schemaFieldPath) {
    return SchemaFieldUtils.generateSchemaFieldUrn(item.getUrn(), schemaFieldPath);
  }
}
