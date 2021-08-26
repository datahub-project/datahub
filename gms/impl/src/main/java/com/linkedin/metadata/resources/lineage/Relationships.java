package com.linkedin.metadata.resources.lineage;

import com.linkedin.common.EntityRelationship;

import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.linkedin.metadata.dao.Neo4jUtil.*;
import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;


/**
 * Rest.li entry point: /relationships?type={entityType}&direction={direction}&types={types}
 */
@RestLiSimpleResource(name = "relationships", namespace = "com.linkedin.lineage")
public final class Relationships extends SimpleResourceTemplate<EntityRelationships> {

    private static final Filter EMPTY_FILTER = new Filter().setCriteria(new CriterionArray());
    private static final Integer MAX_DOWNSTREAM_CNT = 100;

    @Inject
    @Named("graphService")
    private GraphService _graphService;

    public Relationships() {
        super();
    }

    private List<Urn> getRelatedEntities(String rawUrn, List<String> relationshipTypes, RelationshipDirection direction) {
        return
                _graphService.findRelatedUrns("", newFilter("urn", rawUrn),
                        "", EMPTY_FILTER,
                        relationshipTypes, createRelationshipFilter(EMPTY_FILTER, direction),
                        0, MAX_DOWNSTREAM_CNT)
                .stream().map(
                        rawRelatedUrn -> {
                            try {
                                return Urn.createFromString(rawRelatedUrn);
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

    @Nonnull
    @RestMethod.Get
    public Task<EntityRelationships> get(
            @QueryParam("urn") @Nonnull String rawUrn,
            @QueryParam("types") @Nonnull String relationshipTypesParam,
            @QueryParam("direction") @Nonnull  String rawDirection
    ) {
        RelationshipDirection direction = RelationshipDirection.valueOf(rawDirection);
        final List<String> relationshipTypes = Arrays.asList(relationshipTypesParam.split(","));
        return RestliUtils.toTask(() -> {
            final List<Urn> relatedEntities = getRelatedEntities(rawUrn, relationshipTypes, direction);

            final EntityRelationshipArray entityArray = new EntityRelationshipArray(
                    relatedEntities.stream().map(
                        entity -> new EntityRelationship()
                                .setEntity(entity)
                    )
                            .collect(Collectors.toList())
            );

            return new EntityRelationships().setEntities(entityArray);
        });
    }

    @Nonnull
    @RestMethod.Delete
    public UpdateResponse delete(
            @QueryParam("urn") @Nonnull String rawUrn
    ) throws Exception {
        _graphService.removeNode(Urn.createFromString(rawUrn));
        return new UpdateResponse(HttpStatus.S_200_OK);
    }
}
