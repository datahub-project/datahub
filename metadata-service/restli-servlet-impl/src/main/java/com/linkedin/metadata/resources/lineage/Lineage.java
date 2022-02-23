package com.linkedin.metadata.resources.lineage;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;


/**
 * Deprecated! Use {@link Relationships} instead.
 *
 * Rest.li entry point: /lineage/{entityKey}?type={entityType}direction={direction}
 */
@RestLiSimpleResource(name = "lineage", namespace = "com.linkedin.lineage")
public final class Lineage extends SimpleResourceTemplate<EntityRelationships> {

    private static final Integer MAX_DOWNSTREAM_CNT = 100;

    private static final List<String> LINEAGE_RELATIONSHIP_TYPES = Arrays.asList(
        "DownstreamOf", "Consumes", "Contains", "TrainedBy");

    private static final List<String> INVERSE_LINEAGE_RELATIONSHIP_TYPES = Arrays.asList(
        "Produces", "MemberOf");

    @Inject
    @Named("graphService")
    private GraphService _graphService;

    public Lineage() {
        super();
    }

    static RelationshipDirection getOppositeDirection(RelationshipDirection direction) {
        if (direction.equals(RelationshipDirection.INCOMING)) {
            return RelationshipDirection.OUTGOING;
        }
        if (direction.equals(RelationshipDirection.OUTGOING)) {
            return RelationshipDirection.INCOMING;
        }
        return direction;
    }

    private List<Urn> getRelatedEntities(String rawUrn, List<String> relationshipTypes, RelationshipDirection direction) {
        return
            _graphService.findRelatedEntities("", newFilter("urn", rawUrn),
                "", QueryUtils.EMPTY_FILTER,
                relationshipTypes, newRelationshipFilter(QueryUtils.EMPTY_FILTER, direction),
                0, MAX_DOWNSTREAM_CNT)
                .getEntities().stream().map(
                entity -> {
                    try {
                        return Urn.createFromString(entity.getUrn());
                    } catch (URISyntaxException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            ).collect(Collectors.toList());
    }

    @Nonnull
    @RestMethod.Get
    @WithSpan
    public Task<EntityRelationships> get(
        @QueryParam("urn") @Nonnull String rawUrn,
        @QueryParam("direction") @Optional @Nullable String rawDirection
    ) throws URISyntaxException {
        RelationshipDirection direction = RelationshipDirection.valueOf(rawDirection);
        return RestliUtil.toTask(() -> {
            final List<Urn> downstreamOfEntities = getRelatedEntities(rawUrn, LINEAGE_RELATIONSHIP_TYPES, direction);
            downstreamOfEntities.addAll(
                getRelatedEntities(rawUrn, INVERSE_LINEAGE_RELATIONSHIP_TYPES, getOppositeDirection(direction)));

            final EntityRelationshipArray entityArray =
                new EntityRelationshipArray(Stream.of(downstreamOfEntities).flatMap(Collection::stream).map(entity -> {
                    return new EntityRelationship().setEntity(entity);
                }).collect(Collectors.toList()));

            return new EntityRelationships().setRelationships(entityArray);
        }, MetricRegistry.name(this.getClass(), "get"));
    }
}
