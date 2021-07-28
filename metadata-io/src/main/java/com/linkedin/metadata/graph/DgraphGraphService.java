package com.linkedin.metadata.graph;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.*;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

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
    private final DgraphSchema _schema;

    public DgraphGraphService(@Nonnull DgraphClient client) {
        this._client = client;
        this._schema = getSchema(client);

        if (this._schema.isEmpty()) {
            Operation setSchema = Operation.newBuilder()
                    .setSchema("" +
                            "<urn>: string @index(hash) @upsert .\n" +
                            "<type>: string @index(hash) .\n" +
                            "<key>: string @index(hash) .\n"
                    )
                    .build();
            this._client.alter(setSchema);
        }
    }

    protected static @Nonnull DgraphSchema getSchema(@Nonnull DgraphClient client) {
        Response response = client.newReadOnlyTransaction().doRequest(
                Request.newBuilder().setQuery("schema { predicate }").build()
        );
        return getSchema(response.getJson().toStringUtf8());
    }

    protected static @Nonnull DgraphSchema getSchema(@Nonnull String json) {
        Map<String, Object> data = getDataFromResponseJson(json);

        Object schemaObj = data.get("schema");
        if (! (schemaObj instanceof List<?>))
            throw new IllegalArgumentException(
                    "The result from Dgraph did not contain a 'schema' field, or that field is not a List"
            );

        List<?> schemaList = (List<?>)schemaObj;
        Set<String> fieldNames = schemaList.stream().flatMap(fieldObj -> {
            if (!(fieldObj instanceof Map))
                return Stream.empty();

            Map<?, ?> fieldMap = (Map<?, ?>) fieldObj;
            if ( ! (fieldMap.containsKey("predicate") && fieldMap.get("predicate") instanceof String))
                return Stream.empty();

            String fieldName = (String) fieldMap.get("predicate");
            return Stream.of(fieldName);
        }).filter(f -> !f.startsWith("dgraph.")).collect(Collectors.toSet());

        Object typesObj = data.get("types");
        if (! (typesObj instanceof List<?>))
            throw new IllegalArgumentException(
                    "The result from Dgraph did not contain a 'types' field, or that field is not a List"
            );

        List<?> types = (List<?>)typesObj;
        Map<String, Set<String>> typeFields = types.stream().flatMap(typeObj -> {
            if (!(typeObj instanceof Map))
                return Stream.empty();

            Map<?, ?> typeMap = (Map<?, ?>) typeObj;
            if ( ! (typeMap.containsKey("fields") && typeMap.containsKey("name") &&
                    typeMap.get("fields") instanceof List<?> && typeMap.get("name") instanceof String))
                return Stream.empty();

            String typeName = (String) typeMap.get("name");
            List<?> fieldsList = (List<?>) typeMap.get("fields");

            Set<String> fields = fieldsList.stream().flatMap(fieldObj -> {
                if (!(fieldObj instanceof Map<?, ?>))
                    return Stream.empty();

                Map<?, ?> fieldMap = (Map<?, ?>) fieldObj;
                if (! (fieldMap.containsKey("name") && fieldMap.get("name") instanceof String))
                    return Stream.empty();

                String fieldName = (String) fieldMap.get("name");
                return Stream.of(fieldName);
            }).filter(f -> !f.startsWith("dgraph.")).collect(Collectors.toSet());
            return Stream.of(Pair.of(typeName, fields));
        }).filter(t -> !t.getKey().startsWith("dgraph.")).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        return new DgraphSchema(fieldNames, typeFields);
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
        String sourceEntityType = getDgraphType(edge.getSource());
        String relationshipType = edge.getRelationshipType();
        if (! _schema.hasField(sourceEntityType, relationshipType)) {
            StringJoiner schema = new StringJoiner("\n");

            // is the field known at all?
            if (! _schema.hasField(relationshipType)) {
                schema.add(String.format("<%s>: [uid] @reverse .", relationshipType));
            }

            // is the type known at all?
            if (! _schema.hasType(sourceEntityType)) {
                _schema.addField(sourceEntityType, "urn");
                _schema.addField(sourceEntityType, "type");
                _schema.addField(sourceEntityType, "key");
            }

            // add this new field
            this._schema.addField(sourceEntityType, relationshipType);

            // update the schema on the Dgraph cluster
            StringJoiner type = new StringJoiner("\n  ");
            _schema.getFields(sourceEntityType).stream().map(t -> "<" + t + ">").forEach(type::add);
            schema.add(String.format("type <%s> {\n%s\n}", sourceEntityType, type));
            log.debug("Adding to schema: " + schema);
            Operation setSchema = Operation.newBuilder().setSchema(schema.toString()).build();
            this._client.alter(setSchema);
        }

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
        StringJoiner mutations = new StringJoiner("\n");
        mutations.add(String.format("uid(src) <dgraph.type> \"%s\" .", getDgraphType(edge.getSource())));
        mutations.add(String.format("uid(src) <urn> \"%s\" .", edge.getSource()));
        mutations.add(String.format("uid(src) <type> \"%s\" .", edge.getSource().getEntityType()));
        mutations.add(String.format("uid(src) <key> \"%s\" .", edge.getSource().getEntityKey()));
        mutations.add(String.format("uid(dst) <dgraph.type> \"%s\" .", getDgraphType(edge.getDestination())));
        mutations.add(String.format("uid(dst) <urn> \"%s\" .", edge.getDestination()));
        mutations.add(String.format("uid(dst) <type> \"%s\" .", edge.getDestination().getEntityType()));
        mutations.add(String.format("uid(dst) <key> \"%s\" .", edge.getDestination().getEntityKey()));
        mutations.add(String.format("uid(src) <%s> uid(dst) .", edge.getRelationshipType()));

        log.debug("Query: " + query);
        log.debug("Mutations: " + mutations);

        // construct the upsert
        Mutation mutation = Mutation.newBuilder()
                .setSetNquads(ByteString.copyFromUtf8(mutations.toString()))
                .build();
        Request request = Request.newBuilder()
                .setQuery(query)
                .addMutations(mutation)
                .setCommitNow(true)
                .build();

        // run the request
        this._client.newTransaction().doRequest(request);
    }

    private static @Nonnull String getDgraphType(@Nonnull Urn urn) {
        return urn.getNamespace() + ":" + urn.getEntityType();
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

        this._client.newTransaction().doRequest(request);
    }

    @Override
    public void removeEdgesFromNode(@Nonnull Urn urn,
                                    @Nonnull List<String> relationshipTypes,
                                    @Nonnull RelationshipFilter relationshipFilter) {
        RelationshipDirection direction = relationshipFilter.getDirection();

        if (direction == RelationshipDirection.OUTGOING || direction == RelationshipDirection.UNDIRECTED)
            removeOutgoingEdgesFromNode(urn, relationshipTypes);

        if (direction == RelationshipDirection.INCOMING || direction == RelationshipDirection.UNDIRECTED)
            removeIncomingEdgesFromNode(urn, relationshipTypes);
    }

    private void removeOutgoingEdgesFromNode(@Nonnull Urn urn,
                                             @Nonnull List<String> relationshipTypes) {
        // TODO: add escape for string values
        String query = String.format("query {\n" +
                "  node as var(func: eq(<urn>, \"%s\"))\n" +
                "}", urn);

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

    private void removeIncomingEdgesFromNode(@Nonnull Urn urn,
                                             @Nonnull List<String> relationshipTypes) {
        // TODO: add escape for string values
        StringJoiner reverseEdges = new StringJoiner("\n    ");
        IntStream.range(0, relationshipTypes.size()).forEach(idx ->
                reverseEdges.add("<~" + relationshipTypes.get(idx) + "> { uids" + ( idx+1 ) + " as uid }")
        );
        String query = String.format("query {\n" +
                "  node as var(func: eq(<urn>, \"%s\"))\n" +
                "\n" +
                "  var(func: uid(node)) @normalize {\n" +
                "    %s\n" +
                "  }\n" +
                "}", urn, reverseEdges);

        StringJoiner deletions = new StringJoiner("\n");
        IntStream.range(0, relationshipTypes.size()).forEach(idx ->
                deletions.add("uid(uids" + (idx + 1) + ") <" + relationshipTypes.get(idx) + "> uid(node) .")
        );

        log.debug("Query: " + query);
        log.debug("Deletions: " + deletions);

        Mutation mutation = Mutation.newBuilder()
                .setDelNquads(ByteString.copyFromUtf8(deletions.toString()))
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
