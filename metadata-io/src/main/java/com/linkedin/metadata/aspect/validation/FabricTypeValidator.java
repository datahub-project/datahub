package com.linkedin.metadata.aspect.validation;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Deprecation;
import com.linkedin.common.FabricTypeNames;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Accepts an environment name in a {@code FabricTypeName} field when it is a built-in {@code
 * FabricType} symbol, or a registered customFabricType that is neither deleted nor deprecated.
 *
 * <p>Only a new use is checked: a value equal to the one already stored is always kept, so turning
 * custom types off, deprecating or deleting a type never blocks updates to entities that already
 * use it in pre-commit. At request time, with custom types off, a proposal carrying a custom env is
 * refused without reading the stored value, so it can block a non-key write to such an entity until
 * custom types are back on. Restore-indices (RESTATE) is not in scope.
 */
@Setter
@Getter
@Accessors(chain = true)
public class FabricTypeValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;

  /** Field names to check, by entity name then aspect name. */
  @Nonnull private Map<String, Map<String, List<String>>> fields;

  /** Aspects with typed URN fields whose URN holds an environment (e.g. lineage upstreams). */
  @Nonnull
  private FabricTypeNameFields.UrnReferences references =
      new FabricTypeNameFields.UrnReferences(List.of(), Map.of(), Set.of(), Map.of());

  /** {@code featureFlags.customFabricTypesEnabled}, read on every batch. */
  @Nonnull private BooleanSupplier customTypesEnabled;

  /**
   * A validator for every {@code FabricTypeName} field in the registry, and every typed URN field
   * whose URN holds one. RESTATE is not a supported operation, so restore-indices never rejects
   * stored data.
   */
  @Nonnull
  public static FabricTypeValidator create(
      @Nonnull EntityRegistry entityRegistry, @Nonnull BooleanSupplier customTypesEnabled) {
    List<FabricTypeNameFields.Field> discovered = FabricTypeNameFields.discover(entityRegistry);
    FabricTypeNameFields.UrnReferences references =
        FabricTypeNameFields.discoverReferences(entityRegistry);
    List<AspectPluginConfig.EntityAspectName> scope =
        new ArrayList<>(FabricTypeNameFields.entityAspectNames(discovered));
    references.getAspects().stream().filter(pair -> !scope.contains(pair)).forEach(scope::add);
    keyedEntityAspectNames(entityRegistry, discovered).stream()
        .filter(pair -> !scope.contains(pair))
        .forEach(scope::add);
    return new FabricTypeValidator()
        .setFields(discovered)
        .setReferences(references)
        .setCustomTypesEnabled(customTypesEnabled)
        .setConfig(
            AspectPluginConfig.builder()
                .className(FabricTypeValidator.class.getName())
                .enabled(true)
                .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT", "PATCH"))
                .supportedEntityAspectNames(scope)
                .build());
  }

  /**
   * Every aspect of an entity whose key holds an environment. An async write carries only the
   * aspects it changes; the key aspect is added by the consumer, so a write such as a dataset
   * status with an unregistered env in its URN must be in scope to be refused at request time.
   */
  private static List<AspectPluginConfig.EntityAspectName> keyedEntityAspectNames(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull List<FabricTypeNameFields.Field> discovered) {
    Set<String> entities =
        discovered.stream()
            .filter(FabricTypeNameFields.Field::isKeyAspect)
            .map(FabricTypeNameFields.Field::getEntityName)
            .collect(Collectors.toSet());
    return entities.stream()
        .flatMap(
            entityName ->
                entityRegistry
                    .findEntitySpec(entityName)
                    .map(
                        spec ->
                            spec.getAspectSpecs().stream()
                                .map(
                                    aspect ->
                                        AspectPluginConfig.EntityAspectName.builder()
                                            .entityName(entityName)
                                            .aspectName(aspect.getName())
                                            .build()))
                    .orElseGet(Stream::empty))
        .collect(Collectors.toList());
  }

  public FabricTypeValidator setFields(@Nonnull List<FabricTypeNameFields.Field> discovered) {
    this.fields =
        discovered.stream()
            .collect(
                Collectors.groupingBy(
                    FabricTypeNameFields.Field::getEntityName,
                    Collectors.groupingBy(
                        FabricTypeNameFields.Field::getAspectName,
                        Collectors.mapping(
                            FabricTypeNameFields.Field::getFieldName, Collectors.toList()))));
    return this;
  }

  /**
   * Rejects, at request time, a new use of an environment that is not built-in, so a typo is
   * refused by the API call instead of by a consumer later. A built-in value needs no I/O. For a
   * non-built-in value, the stored aspect that holds it (the item's own aspect, or the entity key
   * for a URN) is read, one batched read per aspect name, to tell a new use from a kept one. That
   * read bypasses the client cache on purpose: the cache's TTLs are per aspect for all callers, and
   * a stale copy here would reject a write that keeps its stored value. The same checks as {@link
   * #validatePreCommitAspects} then run. PATCH has no record before it is applied, so it is only
   * checked in pre-commit, which remains the authoritative check.
   *
   * <p>With custom types off every non-built-in value is refused, so nothing is read: the stored
   * value cannot make it acceptable. That refuses a write to an existing custom-env entity's
   * non-key aspect while custom types are off; pre-commit still keeps stored values. A built-in env
   * needs no read either way.
   */
  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    List<Candidate> candidates = new ArrayList<>();
    List<Use> refusedWhenOff = new ArrayList<>();
    for (BatchItem item : mcpItems) {
      // A PATCH has no record before it is applied, and its aspect is JSON patch content that
      // getRecordTemplate cannot deserialize. Pre-commit checks it once applied.
      if (ChangeType.PATCH.equals(item.getChangeType()) || item.getRecordTemplate() == null) {
        continue;
      }
      DataMap record = item.getRecordTemplate().data();
      List<Use> own = new ArrayList<>();
      collectCustomUses(item, record, null, own);
      List<Use> inUrn = new ArrayList<>();
      collectUrnUses(item, null, inUrn);
      if (!own.isEmpty() || !inUrn.isEmpty()) {
        candidates.add(new Candidate(item, !own.isEmpty(), !inUrn.isEmpty()));
        refusedWhenOff.addAll(own);
        refusedWhenOff.addAll(inUrn);
      }
    }
    if (candidates.isEmpty()) {
      return Stream.empty();
    }
    if (!customTypesEnabled.getAsBoolean()) {
      return evaluate(refusedWhenOff, operationContext, retrieverContext);
    }

    // Read only what a decision needs, aspect by aspect: the item's own aspect when it holds the
    // value, and the small key aspect when the value is in the URN. Reading both for every item
    // would fetch large aspects such as schemaMetadata just to find that nothing changed.
    Map<String, Set<Urn>> urnsByAspect = new HashMap<>();
    for (Candidate candidate : candidates) {
      if (candidate.ownUse) {
        urnsByAspect
            .computeIfAbsent(candidate.item.getAspectName(), name -> new HashSet<>())
            .add(candidate.item.getUrn());
      }
      if (candidate.urnUse) {
        urnsByAspect
            .computeIfAbsent(
                candidate.item.getEntitySpec().getKeyAspectName(), name -> new HashSet<>())
            .add(candidate.item.getUrn());
      }
    }
    Map<String, Map<Urn, Map<String, Aspect>>> stored = new HashMap<>();
    urnsByAspect.forEach(
        (aspectName, urns) ->
            stored.put(
                aspectName,
                retrieverContext
                    .getAspectRetriever()
                    .getLatestAspectObjects(operationContext, urns, Set.of(aspectName))));

    List<Use> uses = new ArrayList<>();
    for (Candidate candidate : candidates) {
      BatchItem item = candidate.item;
      if (candidate.ownUse) {
        collectCustomUses(
            item,
            item.getRecordTemplate().data(),
            storedData(stored, item.getUrn(), item.getAspectName()),
            uses);
      }
      if (candidate.urnUse) {
        collectUrnUses(
            item, storedData(stored, item.getUrn(), item.getEntitySpec().getKeyAspectName()), uses);
      }
    }
    return evaluate(uses, operationContext, retrieverContext);
  }

  @Nullable
  private static DataMap storedData(
      @Nonnull Map<String, Map<Urn, Map<String, Aspect>>> stored,
      @Nonnull Urn urn,
      @Nonnull String aspectName) {
    Map<String, Aspect> aspects = stored.getOrDefault(aspectName, Map.of()).get(urn);
    return aspects == null ? null : dataOf(aspects.get(aspectName));
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    List<Use> uses = new ArrayList<>();
    // Proposal time skips PATCH, whose body cannot be deserialized. Once applied, the env may
    // still exist only in the URN (a status write, for example). Read the stored key, bypassing
    // the client cache, to tell that new use from one this entity already has.
    List<Use> urnUses = new ArrayList<>();
    List<ChangeMCP> urnCandidates = new ArrayList<>();
    for (ChangeMCP item : changeMCPs) {
      RecordTemplate record = item.getRecordTemplate();
      if (record != null) {
        RecordTemplate previous = item.getPreviousRecordTemplate();
        collectCustomUses(item, record.data(), previous == null ? null : previous.data(), uses);
      }
      int before = urnUses.size();
      collectUrnUses(item, null, urnUses);
      if (urnUses.size() > before) {
        urnCandidates.add(item);
      }
    }
    if (!urnCandidates.isEmpty()) {
      Map<String, Set<Urn>> urnsByAspect = new HashMap<>();
      for (ChangeMCP item : urnCandidates) {
        urnsByAspect
            .computeIfAbsent(item.getEntitySpec().getKeyAspectName(), name -> new HashSet<>())
            .add(item.getUrn());
      }
      Map<String, Map<Urn, Map<String, Aspect>>> stored = new HashMap<>();
      urnsByAspect.forEach(
          (aspectName, urns) ->
              stored.put(
                  aspectName,
                  retrieverContext
                      .getAspectRetriever()
                      .getLatestAspectObjects(operationContext, urns, Set.of(aspectName))));
      urnUses.clear();
      for (ChangeMCP item : urnCandidates) {
        collectUrnUses(
            item,
            storedData(stored, item.getUrn(), item.getEntitySpec().getKeyAspectName()),
            urnUses);
      }
    }
    uses.addAll(urnUses);
    return evaluate(uses, operationContext, retrieverContext);
  }

  @Nonnull
  private Stream<AspectValidationException> evaluate(
      @Nonnull List<Use> customUses,
      @Nonnull OperationFingerprint operationContext,
      @Nonnull RetrieverContext retrieverContext) {
    if (customUses.isEmpty()) {
      return Stream.empty();
    }

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    if (!customTypesEnabled.getAsBoolean()) {
      customUses.forEach(
          use ->
              exceptions.addException(
                  AspectValidationException.forItem(
                      use.item,
                      String.format(
                          "Environment \"%s\"%s is not a built-in environment, and custom"
                              + " environments are not enabled on this server.",
                          use.value, use.where()))));
      return exceptions.streamAllExceptions();
    }

    Map<String, FabricTypeLookup.Resolution> resolved =
        FabricTypeLookup.resolve(
            operationContext,
            customUses.stream().map(use -> use.value).collect(Collectors.toSet()),
            retrieverContext);
    customUses.forEach(use -> check(use, resolved.get(use.value), exceptions));
    return exceptions.streamAllExceptions();
  }

  @Nullable
  private static DataMap dataOf(@Nullable Aspect aspect) {
    return aspect == null ? null : aspect.data();
  }

  /**
   * Adds the environment in the URN of an entity whose key holds one, unless the stored key has it
   * already. A key aspect is only written to the batch after request-time validation, so the URN is
   * where a new entity's environment is first visible.
   *
   * @param previousKey the stored key aspect of the entity, if known
   */
  private void collectUrnUses(
      @Nonnull BatchItem item, @Nullable DataMap previousKey, @Nonnull List<Use> uses) {
    String keyAspectName = item.getEntitySpec().getKeyAspectName();
    List<String> keyFields =
        fields.getOrDefault(item.getEntitySpec().getName(), Map.of()).get(keyAspectName);
    if (keyFields == null || keyAspectName.equals(item.getAspectName())) {
      return;
    }
    List<String> parts = item.getUrn().getEntityKey().getParts();
    // The key aspect's fields are in the order of the URN's parts.
    RecordDataSchema keySchema = item.getEntitySpec().getKeyAspectSpec().getPegasusSchema();
    for (String fieldName : keyFields) {
      int index = keySchema.getFields().indexOf(keySchema.getField(fieldName));
      String value = index >= 0 && index < parts.size() ? parts.get(index) : null;
      if (value == null || FabricTypeNames.isBuiltIn(value)) {
        continue;
      }
      if (previousKey != null && value.equals(previousKey.getString(fieldName))) {
        continue;
      }
      uses.add(new Use(item, value, null));
    }
  }

  /**
   * Adds each non-built-in value the item writes that differs from the stored one: in its own
   * environment fields, and in the URNs it references.
   */
  private void collectCustomUses(
      @Nonnull BatchItem item,
      @Nonnull DataMap record,
      @Nullable DataMap previous,
      @Nonnull List<Use> uses) {
    List<String> fieldNames =
        fields.getOrDefault(item.getEntitySpec().getName(), Map.of()).get(item.getAspectName());
    if (fieldNames != null) {
      for (String fieldName : fieldNames) {
        String value = record.getString(fieldName);
        if (value == null || FabricTypeNames.isBuiltIn(value)) {
          continue;
        }
        if (previous != null && value.equals(previous.getString(fieldName))) {
          continue;
        }
        uses.add(new Use(item, value, null));
      }
    }
    if (references.contains(item.getEntitySpec().getName(), item.getAspectName())) {
      RecordDataSchema schema =
          item.getEntitySpec().getAspectSpec(item.getAspectName()).getPegasusSchema();
      Map<String, String> referenced = references.customEnvs(record, schema);
      if (!referenced.isEmpty() && previous != null) {
        referenced.keySet().removeAll(references.customEnvs(previous, schema).keySet());
      }
      referenced.forEach((urn, env) -> uses.add(new Use(item, env, urn)));
    }
  }

  private static void check(
      @Nonnull Use use,
      @Nonnull FabricTypeLookup.Resolution resolution,
      @Nonnull ValidationExceptionCollection exceptions) {
    String message = null;
    switch (resolution.getState()) {
      case MALFORMED:
        message =
            String.format(
                "Environment \"%s\"%s is not valid: custom environments use uppercase"
                    + " letters, digits and underscores, starting with a letter.",
                use.value, use.where());
        break;
      case UNREGISTERED:
        message =
            String.format(
                "Environment \"%s\"%s is not registered. Create %s first.",
                use.value, use.where(), FabricTypeLookup.urn(use.value));
        break;
      case DELETED:
        message = String.format("Environment \"%s\"%s is deleted.", use.value, use.where());
        break;
      case REGISTERED:
        // A reference points at an entity that may already exist in the deprecated environment, so
        // only an entity's own environment counts as a new use of it.
        if (use.referencedUrn == null && resolution.getDeprecation() != null) {
          message = deprecatedMessage(use.value, resolution.getDeprecation());
        }
        break;
      default:
        break;
    }
    if (message != null) {
      exceptions.addException(AspectValidationException.forItem(use.item, message));
    }
  }

  @Nonnull
  private static String deprecatedMessage(@Nonnull String value, @Nonnull Deprecation deprecation) {
    StringBuilder builder =
        new StringBuilder(
            String.format("Environment \"%s\" is deprecated and can't be newly used.", value));
    if (deprecation.getNote() != null && !deprecation.getNote().isBlank()) {
      builder.append(' ').append(deprecation.getNote().trim());
    }
    if (deprecation.hasReplacement()) {
      builder.append(" Use ").append(deprecation.getReplacement()).append(" instead.");
    }
    return builder.toString();
  }

  /** A proposed item that may make a new use, and where: in its own aspect, in its URN, or both. */
  private static final class Candidate {
    private final BatchItem item;
    private final boolean ownUse;
    private final boolean urnUse;

    private Candidate(BatchItem item, boolean ownUse, boolean urnUse) {
      this.item = item;
      this.ownUse = ownUse;
      this.urnUse = urnUse;
    }
  }

  private static final class Use {
    private final BatchItem item;
    private final String value;

    /** The referenced URN holding the value, or null for the entity's own environment. */
    @Nullable private final String referencedUrn;

    private Use(@Nonnull BatchItem item, @Nonnull String value, @Nullable String referencedUrn) {
      this.item = item;
      this.value = Objects.requireNonNull(value);
      this.referencedUrn = referencedUrn;
    }

    private String where() {
      return referencedUrn == null ? "" : " (in referenced " + referencedUrn + ")";
    }
  }
}
