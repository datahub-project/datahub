package com.linkedin.metadata.resources.entity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.experimental.Entity;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.EntityDao;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.linkedin.metadata.dao.BaseReadDAO.LATEST_VERSION;
import static com.linkedin.metadata.restli.RestliConstants.*;

/**
 * Single unified resource for fetching, updating, searching, & browsing DataHub entities
 */
@RestLiCollection(name = "entities", namespace = "com.linkedin.entity")
public class EntityResource extends CollectionResourceTaskTemplate<String, Entity> {

    private final Map<String, EntitySpec> _entityNameToSpec;

    @Inject
    @Named("entityDao")
    private EntityDao _entityDao;

    public EntityResource() {
        _entityNameToSpec = new SnapshotEntityRegistry().getEntitySpecs().stream().collect(Collectors.toMap(
                spec -> spec.getName().toLowerCase(),
                spec -> spec
        ));
    }

    /**
     * Retrieves the value for an entity that is made up of latest versions of specified aspects.
     */
    @RestMethod.Get
    @Nonnull
    public Task<Entity> get(@Nonnull String urnStr,
                            @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) throws URISyntaxException {

        final Urn urn = Urn.createFromString(urnStr);
        final String entityName = urn.getEntityType();

        return RestliUtils.toTask(() -> {
            final RecordTemplate value = getInternalNonEmpty(entityName, Collections.singleton(urn), parseAspectsParam(entityName, aspectNames)).get(urn);
            if (value == null) {
                throw RestliUtils.resourceNotFoundException();
            }
            return new Entity().setValue(newSnapshotUnion(value));
        });
    }

    @RestMethod.BatchGet
    @Nonnull
    public Task<Map<String, Entity>> batchGet(
            @Nonnull Set<String> urnStrs,
            @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
        return RestliUtils.toTask(() -> {
            final List<Urn> urns = urnStrs.stream().map(urnStr -> {
                    try {
                        return Urn.createFromString(urnStr);
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(String.format("Failed to create valid urn from string %s", urnStr));
                    }
                }).collect(Collectors.toList());

            final String entityName = urns.get(0).getEntityType();

            return getInternal(entityName, urns, parseAspectsParam(entityName, aspectNames)).entrySet()
                    .stream()
                    .collect(Collectors.toMap(e -> e.getKey().toString(), e -> new Entity().setValue(newSnapshotUnion(e.getValue()))));
        });
    }

    @Action(name = ACTION_INGEST)
    @Nonnull
    public Task<Void> ingest(@ActionParam("entity") @Nonnull Entity entity) {
        return RestliUtils.toTask(() -> {

            final RecordTemplate entitySnapshot = getEntitySnapshot(entity);
            final Urn urn = ModelUtils.getUrnFromSnapshot(entitySnapshot);
            final String entityName = urn.getEntityType();
            final AuditStamp auditStamp = new AuditStamp();
            auditStamp.setTime(123L);
            try {
                auditStamp.setActor(Urn.createFromString("urn:li:principal:test")); // TODO Properly format this.
            } catch (URISyntaxException e) {
                e.printStackTrace();
                throw new RuntimeException("failed to create actor urn");
            }

            ModelUtils.getAspectsFromSnapshot(entitySnapshot).stream().forEach(aspect -> {
                _entityDao.add(entityName, urn, aspect, auditStamp);
            });

            return null;
        });
    }

    private RecordTemplate getEntitySnapshot(final Entity entity) {
        final Snapshot snapshotUnion = entity.getValue();
        // Now, extract the correct field based on the type.
        // This is pretty awful btw
        return RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion);
    }

    @Nonnull
    protected Set<Class<? extends RecordTemplate>> parseAspectsParam(@Nullable String entityName,
                                                                     @Nullable String[] aspectNames) {
        if (aspectNames == null) {
            return getAspectClassesForEntity(entityName);
        }
        return Arrays.asList(aspectNames).stream().map(ModelUtils::getAspectClass).collect(Collectors.toSet());
    }

    private Set<Class<? extends RecordTemplate>> getAspectClassesForEntity(@Nonnull String entityName) {
        final EntitySpec spec = _entityNameToSpec.get(entityName.toLowerCase());
        return spec.getAspectSpecs().stream().map(
                aspectSpec -> (Class<RecordTemplate>) getDataTemplateClassFromSchema(aspectSpec.getPegasusSchema())
        ).collect(Collectors.toSet());
    }

    @Nonnull
    protected Map<Urn, RecordTemplate> getInternal(@Nonnull String entityName,
                                             @Nonnull Collection<Urn> urns,
                                             @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
        return getUrnAspectMap(entityName, urns, aspectClasses).entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> newSnapshot(entityName, e.getKey(), e.getValue())));
    }

    @Nonnull
    protected Map<Urn, RecordTemplate> getInternalNonEmpty(@Nonnull String entityName,
                                                     @Nonnull Collection<Urn> urns,
                                                     @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
        return getUrnAspectMap(entityName, urns, aspectClasses).entrySet()
                .stream()
                .filter(e -> !e.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> newSnapshot(entityName, e.getKey(), e.getValue())));
    }

    @Nonnull
    private Map<Urn, List<UnionTemplate>> getUrnAspectMap(@Nonnull String entityName,
                                                          @Nonnull Collection<Urn> urns,
                                                          @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
        // Construct the keys to retrieve latest version of all supported aspects for all URNs.

        final EntitySpec spec = _entityNameToSpec.get(entityName);

        final Set<AspectKey<Urn, ? extends RecordTemplate>> keys = urns.stream()
                .map(urn -> aspectClasses.stream()
                        .map(clazz -> new AspectKey<>(clazz, urn, LATEST_VERSION))
                        .collect(Collectors.toList()))
                .flatMap(List::stream)
                .collect(Collectors.toSet());

        final Map<Urn, List<UnionTemplate>> urnAspectsMap =
                urns.stream().collect(Collectors.toMap(Function.identity(), urn -> new ArrayList<>()));

        _entityDao.get(entityName, keys).forEach((key, aspect) -> aspect.ifPresent(
                        metadata -> urnAspectsMap.get(key.getUrn())
                            .add(ModelUtils.newAspectUnion(
                                (Class<UnionTemplate>) getDataTemplateClassFromSchema(spec.getAspectTyperefSchema()),
                                metadata
                            ))));

        return urnAspectsMap;
    }

    @Nonnull
    private RecordTemplate newSnapshot(@Nonnull String entityName, @Nonnull Urn urn, @Nonnull List<UnionTemplate> aspects) {
        final EntitySpec spec = _entityNameToSpec.get(entityName);
        return ModelUtils.newSnapshot((Class<RecordTemplate>) getDataTemplateClassFromSchema(spec.getSnapshotSchema()), urn, aspects);
    }

    private Class<? extends DataTemplate> getDataTemplateClassFromSchema(final NamedDataSchema schema) {
        Class<? extends DataTemplate> clazz;
        try {
            clazz = Class.forName(schema.getFullName()).asSubclass(DataTemplate.class);
        } catch (ClassNotFoundException e) {
            throw new ModelConversionException("Unable to find class " + schema.getFullName(), e);
        }
        return clazz;
    }


    @Nonnull
    public static <SNAPSHOT extends RecordTemplate> Snapshot newSnapshotUnion(@Nonnull SNAPSHOT snapshot) {
        Snapshot snapshotUnion = new Snapshot();
        RecordUtils.setSelectedRecordTemplateInUnion(snapshotUnion, snapshot);
        return snapshotUnion;
    }

    /**
     * Creates a snapshot of the entity with no aspects set, just the URN.
     */
    @Nonnull
    private RecordTemplate newSnapshot(@Nonnull String entityName, @Nonnull Urn urn) {
        return newSnapshot(entityName, urn, Collections.emptyList());
    }
}
