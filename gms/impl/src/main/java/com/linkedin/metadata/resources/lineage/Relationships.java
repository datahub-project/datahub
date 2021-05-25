package com.linkedin.metadata.resources.lineage;

import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.BaseQueryDAO;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.relationship.Consumes;
import com.linkedin.metadata.relationship.Contains;
import com.linkedin.metadata.relationship.DownstreamOf;
import com.linkedin.metadata.relationship.IsPartOf;
import com.linkedin.metadata.relationship.Produces;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;
import org.elasticsearch.common.collect.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.linkedin.metadata.dao.Neo4jUtil.createRelationshipFilter;
import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;


/**
 * Rest.li entry point: /relationships?type={entityType}&direction={direction}&relationshipTypes={types}
 */
@RestLiSimpleResource(name = "relationships", namespace = "com.linkedin.lineage")
public final class Relationships extends SimpleResourceTemplate<EntityRelationships> {

    private static final Filter EMPTY_FILTER = new Filter().setCriteria(new CriterionArray());
    private static final Integer MAX_DOWNSTREAM_CNT = 100;

    @Inject
    @Named("datasetQueryDao")
    private BaseQueryDAO _graphQueryDao;

    public Relationships() {
        super();
    }

    private List<Urn> getRelatedEntities(String rawUrn, Class<? extends RecordTemplate> relationshipType, RelationshipDirection direction) {
        return _graphQueryDao
                .findEntities(null, newFilter("urn", rawUrn),
                        null, EMPTY_FILTER,
                        relationshipType, createRelationshipFilter(EMPTY_FILTER, direction),
                        0, MAX_DOWNSTREAM_CNT)
                .stream().map(
                        entity -> {
                            try {
                                return Urn.createFromString((String) entity.data().get("urn"));
                            } catch (URISyntaxException e) {
                                e.printStackTrace();
                            }
                            return null;
                        }
                ).collect(Collectors.toList());
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

    public static java.util.Map<String, Class<? extends RecordTemplate>> relationshipMap = Map.of(
            "DownstreamOf", DownstreamOf.class,
            "Contains", Contains.class,
            "Consumes", Consumes.class,
            "Produces", Produces.class,
            "IsPartOf", IsPartOf.class
    );

    @Nonnull
    @RestMethod.Get
    public Task<EntityRelationships> get(
            @QueryParam("urn") @Nonnull String rawUrn,
            @QueryParam("types") @Nonnull String relationshipTypesParam,
            @QueryParam("direction") @Optional @Nullable String rawDirection
    ) throws URISyntaxException {
        RelationshipDirection direction = RelationshipDirection.valueOf(rawDirection);
        final List<String> relationshipTypes = Arrays.asList(relationshipTypesParam.split(","));
        return RestliUtils.toTask(() -> {
            final List<List<Urn>> relatedEntities = relationshipTypes.stream()
                    .map(relationshipType ->
                            getRelatedEntities(rawUrn, relationshipMap.get(relationshipType), direction))
                    .collect(Collectors.toList());

            final EntityRelationshipArray entityArray = new EntityRelationshipArray(
                    relatedEntities.stream().flatMap(entities -> {
                        return entities.stream().map(entity -> new EntityRelationship()
                                .setEntity(entity));
                    })
                            .collect(Collectors.toList())
            );

            return new EntityRelationships().setEntities(entityArray);
        });
    }
}