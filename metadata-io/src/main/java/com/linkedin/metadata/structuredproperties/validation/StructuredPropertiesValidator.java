package com.linkedin.metadata.structuredproperties.validation;

import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.models.StructuredPropertyUtils.getLogicalValueType;
import static com.linkedin.metadata.models.StructuredPropertyUtils.getValueTypeId;
import static com.linkedin.metadata.structuredproperties.validation.PropertyDefinitionValidator.softDeleteCheck;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.DataPlatformInstanceUtils;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/** A Validator for StructuredProperties Aspect that is attached to entities like Datasets, etc. */
@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class StructuredPropertiesValidator extends AspectPayloadValidator {
  private static final Set<ChangeType> CHANGE_TYPES =
      ImmutableSet.of(
          ChangeType.CREATE, ChangeType.CREATE_ENTITY, ChangeType.UPSERT, ChangeType.UPDATE);

  private static final Set<LogicalValueType> VALID_VALUE_STORED_AS_STRING =
      new HashSet<>(
          Arrays.asList(
              LogicalValueType.STRING,
              LogicalValueType.RICH_TEXT,
              LogicalValueType.DATE,
              LogicalValueType.URN));

  @Nonnull private AspectPluginConfig config;

  /**
   * When true, skip validation for assignments whose definition is missing and reject writes that
   * would retain no valid assignments (aligned with {@link
   * com.linkedin.metadata.structuredproperties.hooks.StructuredPropertiesAssignmentMutator}).
   */
  private boolean dropMissingPropertyValuesWithWarning = false;

  /** Max UTF-8 bytes for string-backed SP values; defaults to Lucene keyword term limit. */
  private int keywordMaxLength = ESUtils.KEYWORD_MAXLENGTH;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    ObjectMapper objectMapper = resolveObjectMapper(operationContext);

    List<BatchItem> upsertItems =
        mcpItems.stream()
            .filter(i -> CHANGE_TYPES.contains(i.getChangeType()))
            .filter(i -> i.getRecordTemplate() != null)
            .collect(Collectors.toList());

    // PATCH has no record template until applyPatch (inside the DB tx). Validate ADD ops here at
    // request time by inspecting the patch payload — ignore REMOVE (no values to check). Route on
    // the change type, not the item class: with alternate MCP validation (the quickstart/docker
    // default, ALTERNATE_MCP_VALIDATION=true) a patch arrives as a ProposedItem carrying the raw
    // proposal rather than a PatchItemImpl.
    List<BatchItem> patchAddItems =
        mcpItems.stream()
            .filter(i -> ChangeType.PATCH.equals(i.getChangeType()))
            .filter(i -> i instanceof MCPItem)
            .map(i -> (MCPItem) i)
            .map(patchItem -> materializePatchAddUpsert(patchItem, aspectRetriever, objectMapper))
            .flatMap(Optional::stream)
            .collect(Collectors.toList());

    List<BatchItem> toValidate = new ArrayList<>(upsertItems.size() + patchAddItems.size());
    toValidate.addAll(upsertItems);
    toValidate.addAll(patchAddItems);

    return validateProposedUpserts(
        operationContext,
        toValidate,
        aspectRetriever,
        dropMissingPropertyValuesWithWarning,
        keywordMaxLength);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    // Immutability needs previous aspect state; value checks stay in validateProposed (including
    // PATCH ADD inspection) so they do not run under the write transaction.
    return validateImmutable(
        operationContext,
        changeMCPs.stream()
            .filter(
                i ->
                    ChangeType.DELETE.equals(i.getChangeType())
                        || CHANGE_TYPES.contains(i.getChangeType()))
            .collect(Collectors.toList()),
        retrieverContext.getAspectRetriever());
  }

  /**
   * Builds a synthetic UPSERT item whose {@link StructuredProperties} contains only assignments
   * from PATCH {@code add} ops, for request-time value validation.
   */
  private static Optional<BatchItem> materializePatchAddUpsert(
      @Nonnull MCPItem patchItem,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull ObjectMapper objectMapper) {
    List<StructuredPropertyValueAssignment> adds =
        extractPatchAddAssignments(patchItem, objectMapper);
    if (adds.isEmpty()) {
      return Optional.empty();
    }
    StructuredProperties props =
        new StructuredProperties().setProperties(new StructuredPropertyValueAssignmentArray(adds));
    AuditStamp auditStamp = patchItem.getAuditStamp();
    if (auditStamp == null) {
      auditStamp = new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L);
    }
    return Optional.of(
        ChangeItemImpl.builder()
            .urn(patchItem.getUrn())
            .aspectName(STRUCTURED_PROPERTIES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .recordTemplate(props)
            .auditStamp(auditStamp)
            .systemMetadata(patchItem.getSystemMetadata())
            .build(aspectRetriever));
  }

  @Nonnull
  public static List<StructuredPropertyValueAssignment> extractPatchAddAssignments(
      @Nonnull MCPItem patchItem, @Nonnull ObjectMapper objectMapper) {
    GenericJsonPatch genericJsonPatch = resolveGenericJsonPatch(patchItem, objectMapper);
    if (genericJsonPatch == null || genericJsonPatch.getPatch() == null) {
      return List.of();
    }
    List<StructuredPropertyValueAssignment> assignments = new ArrayList<>();
    for (GenericJsonPatch.PatchOp op : genericJsonPatch.getPatch()) {
      if (op.getOp() == null || !"add".equalsIgnoreCase(op.getOp()) || op.getValue() == null) {
        continue;
      }
      if (op.getPath() == null || !op.getPath().startsWith("/properties")) {
        continue;
      }
      StructuredPropertyValueAssignment assignment =
          parseAssignmentFromPatchValue(op.getValue(), objectMapper);
      if (assignment != null) {
        assignments.add(assignment);
      } else {
        log.warn(
            "Skipping unparseable structured property PATCH add on {} path={}",
            patchItem.getUrn(),
            op.getPath());
      }
    }
    return assignments;
  }

  /**
   * The patch, regardless of how the item was constructed: a {@link PatchItemImpl} carries it
   * parsed, while under alternate MCP validation the item is a raw proposal whose aspect payload is
   * the serialized {@link GenericJsonPatch} (or a bare json-patch ops array).
   */
  @Nullable
  private static GenericJsonPatch resolveGenericJsonPatch(
      @Nonnull MCPItem patchItem, @Nonnull ObjectMapper objectMapper) {
    if (patchItem instanceof PatchItemImpl) {
      return ((PatchItemImpl) patchItem).getGenericJsonPatch();
    }
    if (patchItem.getMetadataChangeProposal() == null
        || !patchItem.getMetadataChangeProposal().hasAspect()) {
      return null;
    }
    String payload =
        patchItem
            .getMetadataChangeProposal()
            .getAspect()
            .getValue()
            .asString(StandardCharsets.UTF_8);
    try {
      JsonNode parsed = objectMapper.readTree(payload);
      if (parsed.isObject()) {
        return objectMapper.treeToValue(parsed, GenericJsonPatch.class);
      }
      if (parsed.isArray()) {
        List<GenericJsonPatch.PatchOp> ops =
            objectMapper.convertValue(
                parsed, new TypeReference<List<GenericJsonPatch.PatchOp>>() {});
        return GenericJsonPatch.builder().patch(ops).build();
      }
    } catch (Exception e) {
      log.warn(
          "Skipping unparseable structured property PATCH payload on {}: {}",
          patchItem.getUrn(),
          e.toString());
    }
    return null;
  }

  @Nonnull
  private static ObjectMapper resolveObjectMapper(@Nonnull OperationFingerprint fingerprint) {
    if (fingerprint instanceof OperationContext) {
      return ((OperationContext) fingerprint).getObjectMapper();
    }
    return ObjectMapperContext.DEFAULT.getObjectMapper();
  }

  @Nullable
  private static StructuredPropertyValueAssignment parseAssignmentFromPatchValue(
      @Nonnull Object value, @Nonnull ObjectMapper objectMapper) {
    try {
      String json = objectMapper.writeValueAsString(value);
      return RecordUtils.toRecordTemplate(StructuredPropertyValueAssignment.class, json);
    } catch (Exception e) {
      log.debug("Failed to parse structured property patch ADD value: {}", e.toString());
      return null;
    }
  }

  public static Stream<AspectValidationException> validateProposedUpserts(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<BatchItem> mcpItems,
      @Nonnull AspectRetriever aspectRetriever) {
    return validateProposedUpserts(
        operationContext, mcpItems, aspectRetriever, false, ESUtils.KEYWORD_MAXLENGTH);
  }

  public static Stream<AspectValidationException> validateProposedUpserts(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<BatchItem> mcpItems,
      @Nonnull AspectRetriever aspectRetriever,
      boolean dropMissingPropertyValuesWithWarning) {
    return validateProposedUpserts(
        operationContext,
        mcpItems,
        aspectRetriever,
        dropMissingPropertyValuesWithWarning,
        ESUtils.KEYWORD_MAXLENGTH);
  }

  public static Stream<AspectValidationException> validateProposedUpserts(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<BatchItem> mcpItems,
      @Nonnull AspectRetriever aspectRetriever,
      boolean dropMissingPropertyValuesWithWarning,
      int keywordMaxLength) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    Map<Urn, Map<String, Aspect>> allStructuredPropertiesAspects =
        fetchPropertyAspects(operationContext, mcpItems, aspectRetriever, exceptions, false);

    // Only fetch dataPlatformInstance aspects when at least one property definition restricts by
    // platform — avoids a DB round-trip for the common case where no properties use
    // allowedPlatforms
    boolean anyPropertyHasAllowedPlatforms =
        allStructuredPropertiesAspects.values().stream()
            .map(aspectMap -> aspectMap.get(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME))
            .filter(Objects::nonNull)
            .map(aspect -> new StructuredPropertyDefinition(aspect.data()))
            .anyMatch(def -> def.hasAllowedPlatforms() && !def.getAllowedPlatforms().isEmpty());

    Map<Urn, Urn> entityPlatformUrns;
    if (anyPropertyHasAllowedPlatforms) {
      Set<Urn> entityUrns = mcpItems.stream().map(BatchItem::getUrn).collect(Collectors.toSet());
      entityPlatformUrns = fetchEntityPlatformUrns(operationContext, entityUrns, aspectRetriever);
    } else {
      entityPlatformUrns = Collections.emptyMap();
    }

    // Validate assignments
    for (BatchItem i : exceptions.successful(mcpItems)) {
      StructuredProperties structuredProperties = i.getAspect(StructuredProperties.class);
      Set<Urn> missingPropertyUrns = Collections.emptySet();
      if (dropMissingPropertyValuesWithWarning) {
        final int assignmentCountBefore =
            structuredProperties.hasProperties() ? structuredProperties.getProperties().size() : 0;
        if (assignmentCountBefore > 0) {
          final Pair<StructuredProperties, Set<Urn>> filtered =
              StructuredPropertyUtils.filterMissingPropertyDefinitions(
                  operationContext, structuredProperties, aspectRetriever);
          missingPropertyUrns = filtered.getSecond();
          final boolean noValidAssignmentsRemain =
              filtered.getFirst().getProperties() == null
                  || filtered.getFirst().getProperties().isEmpty();
          if (noValidAssignmentsRemain && !missingPropertyUrns.isEmpty()) {
            exceptions.addException(
                i,
                String.format(
                    "Structured properties write rejected for %s: no valid property assignments"
                        + " remain after removing values for non-existent properties: %s",
                    i.getUrn(), missingPropertyUrns));
            continue;
          }
        }
      }

      for (StructuredPropertyValueAssignment structuredPropertyValueAssignment :
          structuredProperties.getProperties()) {

        Urn propertyUrn = structuredPropertyValueAssignment.getPropertyUrn();
        if (dropMissingPropertyValuesWithWarning && missingPropertyUrns.contains(propertyUrn)) {
          log.warn(
              "Skipping validation for non-existent structured property definition {} on {}",
              propertyUrn,
              i.getUrn());
          continue;
        }

        Map<String, Aspect> propertyAspects =
            allStructuredPropertiesAspects.getOrDefault(propertyUrn, Collections.emptyMap());

        // check definition soft delete
        softDeleteCheck(i, propertyAspects, "Cannot apply a soft deleted Structured Property value")
            .ifPresent(exceptions::addException);

        StructuredPropertyDefinition structuredPropertyDefinition =
            lookupPropertyDefinition(propertyUrn, allStructuredPropertiesAspects);
        if (structuredPropertyDefinition == null) {
          exceptions.addException(
              i,
              String.format(
                  "Unexpected null value found for %s Structured Property Definition.",
                  propertyUrn));
          continue;
        }

        log.debug(
            "Retrieved property definition for {}. {}", propertyUrn, structuredPropertyDefinition);
        PrimitivePropertyValueArray values = structuredPropertyValueAssignment.getValues();
        // Check cardinality
        if (structuredPropertyDefinition.getCardinality() == PropertyCardinality.SINGLE) {
          if (values.size() > 1) {
            exceptions.addException(
                i,
                "Property: "
                    + propertyUrn
                    + " has cardinality 1, but multiple values were assigned: "
                    + values);
          }
        }

        // Check values
        for (PrimitivePropertyValue value : values) {
          validateType(i, propertyUrn, structuredPropertyDefinition, value)
              .ifPresent(exceptions::addException);
          validateAllowedValues(i, propertyUrn, structuredPropertyDefinition, value)
              .ifPresent(exceptions::addException);
          validateKeywordByteLength(
                  i, propertyUrn, structuredPropertyDefinition, value, keywordMaxLength)
              .ifPresent(exceptions::addException);
        }

        // Check allowed platforms
        validateAllowedPlatforms(i, propertyUrn, structuredPropertyDefinition, entityPlatformUrns)
            .ifPresent(exceptions::addException);
      }
    }

    return exceptions.streamAllExceptions();
  }

  public static Stream<AspectValidationException> validateImmutable(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull AspectRetriever aspectRetriever) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    final Map<Urn, Map<String, Aspect>> allStructuredPropertiesAspects =
        fetchPropertyAspects(operationContext, changeMCPs, aspectRetriever, exceptions, true);

    Set<Urn> immutablePropertyUrns =
        allStructuredPropertiesAspects.keySet().stream()
            .map(
                stringAspectMap ->
                    Pair.of(
                        stringAspectMap,
                        lookupPropertyDefinition(stringAspectMap, allStructuredPropertiesAspects)))
            .filter(defPair -> defPair.getSecond() != null && defPair.getSecond().isImmutable())
            .map(Pair::getFirst)
            .collect(Collectors.toSet());

    // Validate immutable assignments
    for (ChangeMCP i : exceptions.successful(changeMCPs)) {

      // only apply immutable validation if previous properties exist
      if (i.getPreviousRecordTemplate() != null) {
        Map<Urn, StructuredPropertyValueAssignment> newImmutablePropertyMap =
            i.getAspect(StructuredProperties.class).getProperties().stream()
                .filter(assign -> immutablePropertyUrns.contains(assign.getPropertyUrn()))
                .collect(
                    Collectors.toMap(
                        StructuredPropertyValueAssignment::getPropertyUrn, Function.identity()));
        Map<Urn, StructuredPropertyValueAssignment> oldImmutablePropertyMap =
            i.getPreviousAspect(StructuredProperties.class).getProperties().stream()
                .filter(assign -> immutablePropertyUrns.contains(assign.getPropertyUrn()))
                .collect(
                    Collectors.toMap(
                        StructuredPropertyValueAssignment::getPropertyUrn, Function.identity()));

        // upsert/mutation path
        newImmutablePropertyMap
            .entrySet()
            .forEach(
                entry -> {
                  Urn propertyUrn = entry.getKey();
                  StructuredPropertyValueAssignment assignment = entry.getValue();

                  if (oldImmutablePropertyMap.containsKey(propertyUrn)
                      && !oldImmutablePropertyMap.get(propertyUrn).equals(assignment)) {
                    exceptions.addException(
                        i, String.format("Cannot mutate an immutable property: %s", propertyUrn));
                  }
                });

        // delete path
        oldImmutablePropertyMap.entrySet().stream()
            .filter(entry -> !newImmutablePropertyMap.containsKey(entry.getKey()))
            .forEach(
                entry ->
                    exceptions.addException(
                        i,
                        String.format("Cannot delete an immutable property %s", entry.getKey())));
      }
    }

    return exceptions.streamAllExceptions();
  }

  private static Set<Urn> validateStructuredPropertyUrns(
      Collection<? extends BatchItem> mcpItems, ValidationExceptionCollection exceptions) {
    Set<Urn> validPropertyUrns = new HashSet<>();

    for (BatchItem i : exceptions.successful(mcpItems)) {
      StructuredProperties structuredProperties = i.getAspect(StructuredProperties.class);

      log.debug("Validator called with {}", structuredProperties);
      Map<Urn, List<StructuredPropertyValueAssignment>> structuredPropertiesMap =
          structuredProperties.getProperties().stream()
              .collect(
                  Collectors.groupingBy(
                      x -> x.getPropertyUrn(),
                      HashMap::new,
                      Collectors.toCollection(ArrayList::new)));
      for (Map.Entry<Urn, List<StructuredPropertyValueAssignment>> entry :
          structuredPropertiesMap.entrySet()) {

        // There should only be one entry per structured property
        List<StructuredPropertyValueAssignment> values = entry.getValue();
        if (values.size() > 1) {
          exceptions.addException(
              i, "Property: " + entry.getKey() + " has multiple entries: " + values);
        } else {
          for (StructuredPropertyValueAssignment structuredPropertyValueAssignment :
              structuredProperties.getProperties()) {
            Urn propertyUrn = structuredPropertyValueAssignment.getPropertyUrn();

            if (!propertyUrn.getEntityType().equals("structuredProperty")) {
              exceptions.addException(
                  i,
                  "Unexpected entity type. Expected: structuredProperty Found: "
                      + propertyUrn.getEntityType());
            } else {
              validPropertyUrns.add(propertyUrn);
            }
          }
        }
      }
    }

    return validPropertyUrns;
  }

  private static Set<Urn> previousStructuredPropertyUrns(Collection<? extends BatchItem> mcpItems) {
    return mcpItems.stream()
        .filter(i -> i instanceof ChangeMCP)
        .map(i -> ((ChangeMCP) i))
        .filter(i -> i.getPreviousRecordTemplate() != null)
        .flatMap(i -> i.getPreviousAspect(StructuredProperties.class).getProperties().stream())
        .map(StructuredPropertyValueAssignment::getPropertyUrn)
        .filter(propertyUrn -> propertyUrn.getEntityType().equals("structuredProperty"))
        .collect(Collectors.toSet());
  }

  private static Optional<AspectValidationException> validateAllowedValues(
      BatchItem item,
      Urn propertyUrn,
      StructuredPropertyDefinition definition,
      PrimitivePropertyValue value) {
    if (definition.getAllowedValues() != null) {
      Set<PrimitivePropertyValue> definedValues =
          definition.getAllowedValues().stream()
              .map(PropertyValue::getValue)
              .collect(Collectors.toSet());
      if (definedValues.stream().noneMatch(definedPrimitive -> definedPrimitive.equals(value))) {
        return Optional.of(
            AspectValidationException.forItem(
                item,
                String.format(
                    "Property: %s, value: %s should be one of %s",
                    propertyUrn, value, definedValues)));
      }
    }
    return Optional.empty();
  }

  /**
   * Rejects string-backed structured property values whose UTF-8 encoding exceeds the configured
   * keyword max length (default {@link ESUtils#KEYWORD_MAXLENGTH}). Oversized values otherwise fail
   * Elasticsearch / OpenSearch indexing with {@code max_bytes_length_exceeded_exception}.
   */
  private static Optional<AspectValidationException> validateKeywordByteLength(
      BatchItem item,
      Urn propertyUrn,
      StructuredPropertyDefinition definition,
      PrimitivePropertyValue value,
      int keywordMaxLength) {
    LogicalValueType typeDefinition = getLogicalValueType(definition.getValueType());
    if (!VALID_VALUE_STORED_AS_STRING.contains(typeDefinition) || value.getString() == null) {
      return Optional.empty();
    }
    int maxLength = keywordMaxLength > 0 ? keywordMaxLength : ESUtils.KEYWORD_MAXLENGTH;
    int byteLength = value.getString().getBytes(StandardCharsets.UTF_8).length;
    if (byteLength > maxLength) {
      return Optional.of(
          AspectValidationException.forItem(
              item,
              String.format(
                  "Property: %s, value is %d bytes which exceeds the maximum of %d UTF-8 bytes"
                      + " for structured property values indexed as Elasticsearch keywords"
                      + " (structuredProperties.keywordMaxLength)",
                  propertyUrn, byteLength, maxLength)));
    }
    return Optional.empty();
  }

  private static Optional<AspectValidationException> validateType(
      BatchItem item,
      Urn propertyUrn,
      StructuredPropertyDefinition definition,
      PrimitivePropertyValue value) {
    Urn valueType = definition.getValueType();
    LogicalValueType typeDefinition = getLogicalValueType(valueType);

    // Primitive Type Validation
    if (VALID_VALUE_STORED_AS_STRING.contains(typeDefinition)) {
      log.debug(
          "Property definition demands a string value. {}, {}", value.isString(), value.isDouble());
      if (value.getString() == null) {
        return Optional.of(
            AspectValidationException.forItem(
                item,
                "Property: "
                    + propertyUrn.toString()
                    + ", value: "
                    + value
                    + " should be a string"));
      } else if (typeDefinition.equals(LogicalValueType.DATE)) {
        if (!StructuredPropertyUtils.isValidDate(value)) {
          return Optional.of(
              AspectValidationException.forItem(
                  item,
                  "Property: "
                      + propertyUrn.toString()
                      + ", value: "
                      + value
                      + " should be a date with format YYYY-MM-DD"));
        }
      } else if (typeDefinition.equals(LogicalValueType.URN)) {
        StringArrayMap valueTypeQualifier = definition.getTypeQualifier();
        Urn typeValue;
        try {
          typeValue = Urn.createFromString(value.getString());
        } catch (URISyntaxException e) {
          return Optional.of(
              AspectValidationException.forItem(
                  item,
                  "Property: " + propertyUrn.toString() + ", value: " + value + " should be an urn",
                  e));
        }
        if (valueTypeQualifier != null) {
          if (valueTypeQualifier.containsKey("allowedTypes")) {
            // Let's get the allowed types and validate that the value is one of those types
            StringArray allowedTypes = valueTypeQualifier.get("allowedTypes");
            boolean matchedAny = false;
            for (String type : allowedTypes) {
              Urn typeUrn = null;
              try {
                typeUrn = Urn.createFromString(type);
              } catch (URISyntaxException e) {

                // we don't expect to have types that we allowed to be written that aren't
                // urns
                throw new RuntimeException(e);
              }
              String allowedEntityName = getValueTypeId(typeUrn);
              if (typeValue.getEntityType().equalsIgnoreCase(allowedEntityName)) {
                matchedAny = true;
              }
            }
            if (!matchedAny) {
              return Optional.of(
                  AspectValidationException.forItem(
                      item,
                      "Property: "
                          + propertyUrn.toString()
                          + ", value: "
                          + value
                          + " is not of any supported urn types:"
                          + allowedTypes));
            }
          }
        }
      }
    } else if (typeDefinition.equals(LogicalValueType.NUMBER)) {
      log.debug("Property definition demands a numeric value. {}, {}", value.isString(), value);
      try {
        Double doubleValue =
            value.getDouble() != null ? value.getDouble() : Double.parseDouble(value.getString());
      } catch (NumberFormatException | NullPointerException e) {
        return Optional.of(
            AspectValidationException.forItem(
                item,
                "Property: "
                    + propertyUrn.toString()
                    + ", value: "
                    + value
                    + " should be a number"));
      }
    } else {
      return Optional.of(
          AspectValidationException.forItem(
              item,
              "Validation support for type "
                  + definition.getValueType()
                  + " is not yet implemented."));
    }

    return Optional.empty();
  }

  private static Map<Urn, Map<String, Aspect>> fetchPropertyAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      AspectRetriever aspectRetriever,
      @Nonnull ValidationExceptionCollection exceptions,
      boolean includePrevious) {

    // Validate propertyUrns
    Set<Urn> validPropertyUrns =
        Stream.concat(
                validateStructuredPropertyUrns(mcpItems, exceptions).stream(),
                includePrevious
                    ? previousStructuredPropertyUrns(mcpItems).stream()
                    : Stream.empty())
            .collect(Collectors.toSet());

    if (validPropertyUrns.isEmpty()) {
      return Collections.emptyMap();
    } else {
      return aspectRetriever.getLatestAspectObjects(
          operationContext,
          validPropertyUrns,
          ImmutableSet.of(
              Constants.STATUS_ASPECT_NAME, STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME));
    }
  }

  @Nullable
  private static StructuredPropertyDefinition lookupPropertyDefinition(
      @Nonnull Urn propertyUrn,
      @Nonnull Map<Urn, Map<String, Aspect>> allStructuredPropertiesAspects) {
    Map<String, Aspect> propertyAspects =
        allStructuredPropertiesAspects.getOrDefault(propertyUrn, Collections.emptyMap());
    Aspect structuredPropertyDefinitionAspect =
        propertyAspects.get(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
    return structuredPropertyDefinitionAspect == null
        ? null
        : new StructuredPropertyDefinition(structuredPropertyDefinitionAspect.data());
  }

  /**
   * Batch-fetches data platform URNs for all entity URNs and returns a map of entity URN to data
   * platform URN.
   *
   * <p>SchemaField entities don't have their own {@code dataPlatformInstance} aspect — the platform
   * is encoded in the embedded dataset URN inside the schemaField URN. For all other entity types,
   * the platform is read from the stored {@code dataPlatformInstance} aspect.
   */
  private static Map<Urn, Urn> fetchEntityPlatformUrns(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Set<Urn> entityUrns,
      @Nonnull AspectRetriever aspectRetriever) {
    if (entityUrns.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<Urn, Urn> result = new HashMap<>();
    Set<Urn> nonSchemaFieldUrns = new HashSet<>();

    for (Urn entityUrn : entityUrns) {
      if (Constants.SCHEMA_FIELD_ENTITY_NAME.equals(entityUrn.getEntityType())) {
        // schemaField URNs embed the parent dataset URN as their id, e.g.:
        //   urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,...),fieldPath)
        // Recover the dataset URN via getId(), then delegate to DataPlatformInstanceUtils.
        try {
          Urn datasetUrn = UrnUtils.getUrn(entityUrn.getId());
          result.put(entityUrn, DataPlatformInstanceUtils.getDataPlatform(datasetUrn));
        } catch (Exception e) {
          log.warn("Failed to extract platform URN from schemaField URN: {}", entityUrn, e);
        }
      } else {
        nonSchemaFieldUrns.add(entityUrn);
      }
    }

    if (!nonSchemaFieldUrns.isEmpty()) {
      Map<Urn, Map<String, Aspect>> platformInstanceAspects =
          aspectRetriever.getLatestAspectObjects(
              operationContext,
              nonSchemaFieldUrns,
              ImmutableSet.of(DATA_PLATFORM_INSTANCE_ASPECT_NAME));
      platformInstanceAspects.forEach(
          (entityUrn, aspectMap) -> {
            Aspect platformInstanceAspect = aspectMap.get(DATA_PLATFORM_INSTANCE_ASPECT_NAME);
            if (platformInstanceAspect != null) {
              DataPlatformInstance dataPlatformInstance =
                  new DataPlatformInstance(platformInstanceAspect.data());
              if (dataPlatformInstance.hasPlatform()) {
                result.put(entityUrn, dataPlatformInstance.getPlatform());
              }
            }
          });
    }

    return result;
  }

  /**
   * Validates that the entity's data platform is in the structured property's allowedPlatforms
   * list. If allowedPlatforms is empty or null, the check passes (applies to all platforms).
   */
  private static Optional<AspectValidationException> validateAllowedPlatforms(
      @Nonnull BatchItem item,
      @Nonnull Urn propertyUrn,
      @Nonnull StructuredPropertyDefinition definition,
      @Nonnull Map<Urn, Urn> entityPlatformUrns) {
    if (!definition.hasAllowedPlatforms() || definition.getAllowedPlatforms().isEmpty()) {
      return Optional.empty();
    }
    Urn entityPlatformUrn = entityPlatformUrns.get(item.getUrn());
    if (entityPlatformUrn == null) {
      return Optional.of(
          AspectValidationException.forItem(
              item,
              String.format(
                  "Property: %s is restricted to specific data platforms %s, but the entity %s"
                      + " does not have a data platform.",
                  propertyUrn, definition.getAllowedPlatforms(), item.getUrn())));
    }
    boolean platformAllowed =
        definition.getAllowedPlatforms().stream()
            .anyMatch(allowedPlatform -> allowedPlatform.equals(entityPlatformUrn));
    if (!platformAllowed) {
      return Optional.of(
          AspectValidationException.forItem(
              item,
              String.format(
                  "Property: %s is restricted to data platforms %s, but the entity %s belongs to"
                      + " platform %s.",
                  propertyUrn,
                  definition.getAllowedPlatforms(),
                  item.getUrn(),
                  entityPlatformUrn)));
    }
    return Optional.empty();
  }
}
