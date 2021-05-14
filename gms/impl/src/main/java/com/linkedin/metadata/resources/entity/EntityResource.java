package com.linkedin.metadata.resources.entity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.experimental.Entity;
import com.linkedin.metadata.dao.EntityService;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;

import java.time.Clock;
import java.util.HashSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linkedin.metadata.restli.RestliConstants.*;

/**
 * Single unified resource for fetching, updating, searching, & browsing DataHub entities
 */
@RestLiCollection(name = "entities", namespace = "com.linkedin.entity")
public class EntityResource extends CollectionResourceTaskTemplate<String, Entity> {

    private static final String ENTITY_PARAM = "entity";
    private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";
    private final Clock _clock = Clock.systemUTC();

    @Inject
    @Named("entityService")
    private EntityService _entityService;

    /**
     * Retrieves the value for an entity that is made up of latest versions of specified aspects.
     */
    @RestMethod.Get
    @Nonnull
    public Task<Entity> get(@Nonnull String urnStr,
                            @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) throws URISyntaxException {
        final Urn urn = Urn.createFromString(urnStr);
        return RestliUtils.toTask(() -> {
            final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(aspectNames));
            final Entity entity = _entityService.getEntity(urn, projectedAspects);
            if (entity == null) {
                throw RestliUtils.resourceNotFoundException();
            }
            return entity;
        });
    }

    @RestMethod.BatchGet
    @Nonnull
    public Task<Map<String, Entity>> batchGet(
            @Nonnull Set<String> urnStrs,
            @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) throws URISyntaxException {
        final Set<Urn> urns = new HashSet<>();
        for (final String urnStr : urnStrs) {
            urns.add(Urn.createFromString(urnStr));
        }
        return RestliUtils.toTask(() -> {
            final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(aspectNames));
            return _entityService.batchGetEntities(urns, projectedAspects).entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
        });
    }

    @Action(name = ACTION_INGEST)
    @Nonnull
    public Task<Void> ingest(@ActionParam(ENTITY_PARAM) @Nonnull Entity entity) throws URISyntaxException {
        // TODO Correctly audit ingestions.
        final AuditStamp auditStamp = new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(DEFAULT_ACTOR));
        return RestliUtils.toTask(() -> {
            _entityService.ingestEntity(entity, auditStamp);
            return null;
        });
    }
}
