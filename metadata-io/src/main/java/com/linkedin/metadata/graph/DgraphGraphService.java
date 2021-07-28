package com.linkedin.metadata.graph;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.*;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.DgraphProto.Value;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.NQuad;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Request;
import io.dgraph.DgraphProto.Response;
import io.dgraph.Helpers;
import io.dgraph.Transaction;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
public class DgraphGraphService implements GraphService {

    private final DgraphClient _client;

    public DgraphGraphService(@Nonnull DgraphClient client) {
        this._client = client;
        Operation setSchema = Operation.newBuilder()
                .setSchema("" +
                        "<urn>: string @index(hash) .\n" +
                        "<type>: string @index(exact) .\n" +
                        "<key>: string @index(hash) .\n"
                )
                .build();
        this._client.alter(setSchema);
    }

    @Override
    public void addEdge(Edge edge) {
        log.debug(String.format("Adding Edge source: %s, destination: %s, type: %s",
                edge.getSource(),
                edge.getDestination(),
                edge.getRelationshipType()));

        // add the relationship type to the schema
        // TODO: translate edge name to allowed dgraph uris
        // TODO: cache the schema and only mutate if relationship is new
        String schema = String.format("<%s>: [uid] @reverse .", edge.getRelationshipType());
        log.debug("Adding to schema: " + schema);
        Operation setSchema = Operation.newBuilder()
                .setSchema(schema)
                .build();
        this._client.alter(setSchema);

        // lookup the source and destination nodes
        // TODO: add escape for string values
        String query = String.format("query {\n" +
                " src as var(func: eq(urn, \"%s\"))\n" +
                " dst as var(func: eq(urn, \"%s\"))\n" +
                "}", edge.getSource(), edge.getDestination());
        // create source and destination nodes if they do not exist
        // and create the new edge between them
        // TODO: add escape for string values
        // TODO: translate edge name to allowed dgraph uris
        String mutations = String.format("" +
                        "uid(src) <urn> \"%s\" .\n" +
                        "uid(src) <type> \"%s\" .\n" +
                        "uid(src) <key> \"%s\" .\n" +
                        "uid(dst) <urn> \"%s\" .\n" +
                        "uid(dst) <type> \"%s\" .\n" +
                        "uid(dst) <key> \"%s\" .\n" +
                        "uid(src) <%s> uid(dst) .",
                edge.getSource(),
                edge.getSource().getEntityType(),
                edge.getSource().getEntityKey(),
                edge.getDestination(),
                edge.getDestination().getEntityType(),
                edge.getDestination().getEntityKey(),
                edge.getRelationshipType()
        );

        log.debug("Query: " + query);
        log.debug("Mutations: " + mutations);

        // construct the upsert
        Mutation mutation = Mutation.newBuilder()
                .setSetNquads(ByteString.copyFromUtf8(mutations))
                .build();
        Request request = Request.newBuilder()
                .setQuery(query)
                .addMutations(mutation)
                .setCommitNow(true)
                .build();

        // run the request
        this._client.newTransaction().doRequest(request);
    }

    private static List<String> getDirectedRelationshipTypes(List<String> relationships,
                                                             RelationshipDirection direction) {

        if (direction == RelationshipDirection.INCOMING || direction == RelationshipDirection.UNDIRECTED) {
            List<String> incomingRelationships = relationships.stream()
                    .map(type -> "~" + type).collect(Collectors.toList());

            if (direction == RelationshipDirection.INCOMING) {
                return incomingRelationships;
            } else {
                relationships = new ArrayList<>(relationships);
                relationships.addAll(incomingRelationships);
            }
        }

        return relationships;
    }

    protected static String getQueryForRelatedUrns(@Nullable String sourceType,
                                                   @Nonnull Filter sourceEntityFilter,
                                                   @Nullable String destinationType,
                                                   @Nonnull Filter destinationEntityFilter,
                                                   @Nonnull List<String> relationshipTypes,
                                                   @Nonnull RelationshipFilter relationshipFilter,
                                                   int offset,
                                                   int count) {
        // TODO: verify assumptions: criterions in filters are AND
        // TODO: support destinationEntityFilter

        final List<String> directedRelationshipTypes = getDirectedRelationshipTypes(
                relationshipTypes, relationshipFilter.getDirection()
        );

        List<String> filters = new ArrayList<>();

        Set<String> sourceNodeFilterNames = new HashSet<>();
        String sourceTypeFilterName = null;
        String destinationTypeFilterName = null;
        List<String> sourceFilterNames = new ArrayList<>();
        List<String> destinationFilterNames = new ArrayList<>();
        List<String> relationshipTypeFilterNames = new ArrayList<>();

        if (sourceType != null && !sourceType.isEmpty()) {
            sourceTypeFilterName = "sourceType";
            // TODO: escape string value
            filters.add(String.format("%s as var(func: eq(<type>, \"%s\"))", sourceTypeFilterName, sourceType));
        }

        if (destinationType != null && !destinationType.isEmpty()) {
            destinationTypeFilterName = "destinationTypeType";
            // TODO: escape string value
            filters.add(String.format("%s as var(func: eq(<type>, \"%s\"))", destinationTypeFilterName, destinationType));
        }

        CriterionArray sourceCriteria = sourceEntityFilter.getCriteria();
        IntStream.range(0, sourceCriteria.size())
                .forEach(idx -> {
                    String sourceFilterName = "sourceFilter" + (idx + 1);
                    sourceFilterNames.add(sourceFilterName);
                    Criterion criterion = sourceCriteria.get(idx);
                    // TODO: escape field name and string value
                    filters.add(String.format("%s as var(func: eq(<%s>, \"%s\"))", sourceFilterName, criterion.getField(), criterion.getValue()));
                });
        CriterionArray destinationCriteria = destinationEntityFilter.getCriteria();
        IntStream.range(0, destinationCriteria.size())
                .forEach(idx -> {
                    String sourceFilterName = "destinationFilter" + (idx + 1);
                    destinationFilterNames.add(sourceFilterName);
                    Criterion criterion = destinationCriteria.get(idx);
                    // TODO: escape field name and string value
                    filters.add(String.format("%s as var(func: eq(<%s>, \"%s\"))", sourceFilterName, criterion.getField(), criterion.getValue()));
                });

        IntStream.range(0, directedRelationshipTypes.size())
                .forEach(idx -> {
                    String relationshipTypeFilterName = "relationshipType" + (idx + 1);
                    relationshipTypeFilterNames.add(relationshipTypeFilterName);
                    // TODO: escape string value
                    filters.add(String.format("%s as var(func: has(<%s>))", relationshipTypeFilterName, directedRelationshipTypes.get(idx)));
                });

        // the source node filter is the first filter that is being applied on the source node
        // we can add multiple filters, they will combine as OR
        if (sourceTypeFilterName != null)
            sourceNodeFilterNames.add(sourceTypeFilterName);
        sourceNodeFilterNames.addAll(sourceFilterNames);
        sourceNodeFilterNames.addAll(relationshipTypeFilterNames);

        // TODO: we can only retrieve all relationships (non given) when we add all relationships to a node type
        //       then we would also have to move the destination type filter to a different place in the query
        //       better not support such a broad query
        // TODO: throw exception when no relationship type given (test for that exception)
        //       then, condition if (bootstrapFilterName == null) can be removed
        // if there is still no bootstrap filter, we filter for all nodes
        if (sourceNodeFilterNames.isEmpty()) {
            sourceNodeFilterNames.add("allNodes");
            String sourceNodeFilter = String.format("%s as var(func: has(<urn>))", sourceNodeFilterNames);
            filters.add(sourceNodeFilter);
        }
        StringJoiner sourceNodeFilterJoiner = new StringJoiner(", ");
        sourceNodeFilterNames.stream().sorted().forEach(sourceNodeFilterJoiner::add);
        String sourceNodeFilter = sourceNodeFilterJoiner.toString();

        String filterConditions = getFilterConditions(
                sourceTypeFilterName, destinationTypeFilterName,
                sourceFilterNames, destinationFilterNames,
                relationshipTypeFilterNames, directedRelationshipTypes
        );

        StringJoiner filterJoiner = new StringJoiner("\n  ");
        filters.forEach(filterJoiner::add);
        String filterExpressions = filterJoiner.toString();

        StringJoiner relationshipJoiner = new StringJoiner("\n    ");
        directedRelationshipTypes.forEach(relationshipType -> relationshipJoiner.add(String.format("<%s> { uid <urn> <type> <key> }", relationshipType)));
        String relationships = relationshipJoiner.toString();
        return String.format("query {\n" +
                        "  %s\n" +
                        "\n" +
                        "  result (func: uid(%s), first: %d, offset: %d) %s {\n" +
                        "    uid\n" +
                        "    <urn>\n" +
                        "    <type>\n" +
                        "    <key>\n" +
                        "    %s\n" +
                        "  }\n" +
                        "}",
                filterExpressions,
                sourceNodeFilter,
                count, offset,
                filterConditions,
                relationships);
    }

    @Nonnull
    @Override
    public List<String> findRelatedUrns(@Nullable String sourceType,
                                        @Nonnull Filter sourceEntityFilter,
                                        @Nullable String destinationType,
                                        @Nonnull Filter destinationEntityFilter,
                                        @Nonnull List<String> relationshipTypes,
                                        @Nonnull RelationshipFilter relationshipFilter,
                                        int offset,
                                        int count) {
        String query = getQueryForRelatedUrns(
                sourceType, sourceEntityFilter,
                destinationType, destinationEntityFilter,
                relationshipTypes, relationshipFilter,
                offset, count
        );

        log.debug("Query: " + query);

        Request request = Request.newBuilder()
                .setQuery(query)
                .build();

        Response response = this._client.newReadOnlyTransaction().doRequest(request);
        String json = response.getJson().toStringUtf8();
        Map<String, Object> data = getDataFromResponseJson(json);

        List<String> directedRelationshipTypes = getDirectedRelationshipTypes(
                relationshipTypes, relationshipFilter.getDirection()
        );

        return getDestinationUrnsFromResponseData(data, directedRelationshipTypes);
    }

    protected static @Nonnull String getFilterConditions(String sourceTypeFilterName,
                                                         String destinationTypeFilterName,
                                                         @Nonnull List<String> sourceFilterNames,
                                                         @Nonnull List<String> destinationFilterNames,
                                                         @Nonnull List<String> relationshipTypeFilterNames,
                                                         @Nonnull List<String> relationshipTypes) {
        if (relationshipTypes.size() != relationshipTypeFilterNames.size())
            throw new IllegalArgumentException("relationshipTypeFilterNames and relationshipTypes " +
                    "must have same size: " + relationshipTypeFilterNames + " vs. " + relationshipTypes);

        if (sourceTypeFilterName== null && destinationTypeFilterName == null &&
                sourceFilterNames.isEmpty() && destinationFilterNames.isEmpty() &&
                relationshipTypeFilterNames.isEmpty())
            return "";

        StringJoiner andJoiner = new StringJoiner(" AND\n    ");
        if (sourceTypeFilterName != null) andJoiner.add(String.format("uid(%s)", sourceTypeFilterName));

        sourceFilterNames.forEach(filter -> andJoiner.add(String.format("uid(%s)", filter)));

        if (!relationshipTypes.isEmpty()) {
            StringJoiner orJoiner = new StringJoiner(" OR\n      ");
            IntStream.range(0, relationshipTypes.size()).forEach(idx -> orJoiner.add(getRelationshipCondition(
                    relationshipTypes.get(idx), relationshipTypeFilterNames.get(idx),
                    destinationTypeFilterName, destinationFilterNames
            )));
            String relationshipCondition = orJoiner.toString();
            andJoiner.add(String.format("(\n      %s\n    )", relationshipCondition));
        }

        String conditions = andJoiner.toString();
        return String.format("@filter(\n    %s\n  )", conditions);
    }

    protected static String getRelationshipCondition(@Nonnull String relationshipType,
                                                     @Nonnull String relationshipTypeFilterName,
                                                     String destinationTypeFilterName,
                                                     @Nonnull List<String> destinationFilterNames) {
        StringJoiner andJoiner = new StringJoiner(" AND ");
        andJoiner.add(String.format("uid(%s)", relationshipTypeFilterName));
        if (destinationTypeFilterName != null)
            andJoiner.add(String.format("uid_in(<%s>, uid(%s))", relationshipType, destinationTypeFilterName));
        destinationFilterNames.forEach(filter -> andJoiner.add(String.format("uid_in(<%s>, uid(%s))", relationshipType, filter)));
        return andJoiner.toString();
    }

    protected static Map<String, Object> getDataFromResponseJson(String json) {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};
        try {
            return mapper.readValue(json, typeRef);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse response json: " + json.substring(0, 1000), e);
        }
    }

    protected static List<String> getDestinationUrnsFromResponseData(Map<String, Object> data, List<String> relationshipTypes) {
        Object obj = data.get("result");
        if (! (obj instanceof List<?>))
            throw new IllegalArgumentException(
                    "The result from Dgraph did not contain a 'result' field, or that field is not a List"
            );

        List<?> results = (List<?>)obj;
        return results.stream().flatMap(sourceObj -> {
            if (!(sourceObj instanceof Map))
                return Stream.empty();

            Map<?, ?> source = (Map<?, ?>) sourceObj;
            return relationshipTypes.stream().flatMap(relationship -> {
                Object destinationObjs = source.get(relationship);
                if (!(destinationObjs instanceof List))
                    return Stream.empty();

                List<?> destinations = (List<?>)destinationObjs;
                return destinations.stream().flatMap(destinationObj -> {
                    if (!(destinationObj instanceof Map))
                        return Stream.empty();

                    Map<?, ?> destination = (Map<?, ?>) destinationObj;
                    Object destinationUrnObj = destination.get("urn");
                    if (!(destinationUrnObj instanceof String))
                        return Stream.empty();

                    return Stream.of((String) destinationUrnObj);
                });
            });
        }).collect(Collectors.toList());
    }

    @Override
    public void removeNode(@Nonnull Urn urn) {
        // TODO: S * * . deletes only predicates that are part of the nodes type
        //       maintain the schema of the node to be able to wildcard delete edges
        String query = String.format("query {\n" +
                " node as var(func: eq(urn, \"%s\"))\n" +
                "}", urn);
        String deletion = "uid(node) * * .";

        log.debug("Query: " + query);
        log.debug("Delete: " + deletion);

        Mutation mutation = Mutation.newBuilder()
                .setDelNquads(ByteString.copyFromUtf8(deletion))
                .build();
        Request request = Request.newBuilder()
                .setQuery(query)
                .addMutations(mutation)
                .setCommitNow(true)
                .build();

        Response response = this._client.newTransaction().doRequest(request);
        ByteString json = response.getJson();
    }

    @Override
    public void removeEdgesFromNode(@Nonnull Urn urn,
                                    @Nonnull List<String> relationshipTypes,
                                    @Nonnull RelationshipFilter relationshipFilter) {
        // TODO: implement reverse relationships
        // TODO: add escape for string values
        String query = String.format("query {\n" +
                "  node as var(func: eq(<urn>, \"%s\"))\n" +
                "}", urn.toString());

        Value star = Value.newBuilder().setDefaultVal("_STAR_ALL").build();
        List<NQuad> deletions = relationshipTypes.stream().map(relationshipType ->
                NQuad.newBuilder()
                        .setSubject("uid(node)")
                        .setPredicate(relationshipType)
                        .setObjectValue(star)
                        .build()
        ).collect(Collectors.toList());

        log.debug("Query: " + query);
        log.debug("Deletions: " + deletions);

        Mutation mutation = Mutation.newBuilder()
                .addAllDel(deletions)
                .build();
        Request request = Request.newBuilder()
                .setQuery(query)
                .addMutations(mutation)
                .setCommitNow(true)
                .build();

        this._client.newTransaction().doRequest(request);
    }

    @Override
    public void configure() {

    }

    @Override
    public void clear() {
        this._client.alter(Operation.newBuilder().setDropOp(Operation.DropOp.DATA).build());
    }
}
