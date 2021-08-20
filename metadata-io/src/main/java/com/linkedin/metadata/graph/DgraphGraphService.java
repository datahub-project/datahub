package com.linkedin.metadata.graph;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.NQuad;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Request;
import io.dgraph.DgraphProto.Response;
import io.dgraph.DgraphProto.Value;
import lombok.Getter;
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

    private final @Nonnull DgraphClient _client;

    @Getter(lazy = true)
    // we want to defer initialization of schema (accessing Dgraph server) to the first time accessing _schema
    private final DgraphSchema _schema = getSchema(_client);

    public DgraphGraphService(@Nonnull DgraphClient client) {
        this._client = client;
    }

    protected static @Nonnull DgraphSchema getSchema(@Nonnull DgraphClient client) {
        Response response = client.newReadOnlyTransaction().doRequest(
                Request.newBuilder().setQuery("schema { predicate }").build()
        );
        DgraphSchema schema = getSchema(response.getJson().toStringUtf8());

        if (schema.isEmpty()) {
            Operation setSchema = Operation.newBuilder()
                    .setSchema(""
                            + "<urn>: string @index(hash) @upsert .\n"
                            + "<type>: string @index(hash) .\n"
                            + "<key>: string @index(hash) .\n"
                    )
                    .build();
            client.alter(setSchema);
        }

        return schema;
    }

    protected static @Nonnull DgraphSchema getSchema(@Nonnull String json) {
        Map<String, Object> data = getDataFromResponseJson(json);

        Object schemaObj = data.get("schema");
        if (!(schemaObj instanceof List<?>)) {
            throw new IllegalArgumentException(
                    "The result from Dgraph did not contain a 'schema' field, or that field is not a List"
            );
        }

        List<?> schemaList = (List<?>) schemaObj;
        Set<String> fieldNames = schemaList.stream().flatMap(fieldObj -> {
            if (!(fieldObj instanceof Map)) {
                return Stream.empty();
            }

            Map<?, ?> fieldMap = (Map<?, ?>) fieldObj;
            if (!(fieldMap.containsKey("predicate") && fieldMap.get("predicate") instanceof String)) {
                return Stream.empty();
            }

            String fieldName = (String) fieldMap.get("predicate");
            return Stream.of(fieldName);
        }).filter(f -> !f.startsWith("dgraph.")).collect(Collectors.toSet());

        Object typesObj = data.get("types");
        if (!(typesObj instanceof List<?>)) {
            throw new IllegalArgumentException(
                    "The result from Dgraph did not contain a 'types' field, or that field is not a List"
            );
        }

        List<?> types = (List<?>) typesObj;
        Map<String, Set<String>> typeFields = types.stream().flatMap(typeObj -> {
            if (!(typeObj instanceof Map)) {
                return Stream.empty();
            }

            Map<?, ?> typeMap = (Map<?, ?>) typeObj;
            if (!(typeMap.containsKey("fields")
                    && typeMap.containsKey("name")
                    && typeMap.get("fields") instanceof List<?>
                    && typeMap.get("name") instanceof String)) {
                return Stream.empty();
            }

            String typeName = (String) typeMap.get("name");
            List<?> fieldsList = (List<?>) typeMap.get("fields");

            Set<String> fields = fieldsList.stream().flatMap(fieldObj -> {
                if (!(fieldObj instanceof Map<?, ?>)) {
                    return Stream.empty();
                }

                Map<?, ?> fieldMap = (Map<?, ?>) fieldObj;
                if (!(fieldMap.containsKey("name") && fieldMap.get("name") instanceof String)) {
                    return Stream.empty();
                }

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
        if (!get_schema().hasField(sourceEntityType, relationshipType)) {
            StringJoiner schema = new StringJoiner("\n");

            // is the field known at all?
            if (!get_schema().hasField(relationshipType)) {
                schema.add(String.format("<%s>: [uid] @reverse .", relationshipType));
            }

            // is the type known at all?
            if (!get_schema().hasType(sourceEntityType)) {
                get_schema().addField(sourceEntityType, "urn");
                get_schema().addField(sourceEntityType, "type");
                get_schema().addField(sourceEntityType, "key");
            }

            // add this new field
            this.get_schema().addField(sourceEntityType, relationshipType);

            // update the schema on the Dgraph cluster
            StringJoiner type = new StringJoiner("\n  ");
            get_schema().getFields(sourceEntityType).stream().map(t -> "<" + t + ">").forEach(type::add);
            schema.add(String.format("type <%s> {\n%s\n}", sourceEntityType, type));
            log.debug("Adding to schema: " + schema);
            Operation setSchema = Operation.newBuilder().setSchema(schema.toString()).build();
            this._client.alter(setSchema);
        }

        // lookup the source and destination nodes
        // TODO: add escape for string values
        String query = String.format("query {\n"
                + " src as var(func: eq(urn, \"%s\"))\n"
                + " dst as var(func: eq(urn, \"%s\"))\n"
                + "}", edge.getSource(), edge.getDestination());
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

    // Returns reversed and directed relationship types:
    // <rel> returns <~rel> on outgoing and <rel> on incoming and both on undirected
    private static List<String> getDirectedRelationshipTypes(List<String> relationships,
                                                             RelationshipDirection direction) {

        if (direction == RelationshipDirection.OUTGOING || direction == RelationshipDirection.UNDIRECTED) {
            List<String> outgoingRelationships = relationships.stream()
                    .map(type -> "~" + type).collect(Collectors.toList());

            if (direction == RelationshipDirection.OUTGOING) {
                return outgoingRelationships;
            } else {
                relationships = new ArrayList<>(relationships);
                relationships.addAll(outgoingRelationships);
            }
        }

        return relationships;
    }

    protected static String getQueryForRelatedEntities(@Nullable String sourceType,
                                                       @Nonnull Filter sourceEntityFilter,
                                                       @Nullable String destinationType,
                                                       @Nonnull Filter destinationEntityFilter,
                                                       @Nonnull List<String> relationshipTypes,
                                                       @Nonnull RelationshipFilter relationshipFilter,
                                                       int offset,
                                                       int count) {
        // TODO: verify assumptions: criterions in filters are AND

        if (relationshipTypes.isEmpty()) {
            // we would have to construct a query that never returns an results
            // just do not call this method in the first place
            throw new IllegalArgumentException("The relationship types must not be empty");
        }

        // We are not querying for <src> <relationship> <dest> and return <dest>
        // but we reverse the relationship and query for <dest> <~relationship> <src>
        // this guarantees there are no duplicates among the returned <dest>s
        final List<String> directedRelationshipTypes = getDirectedRelationshipTypes(
                relationshipTypes, relationshipFilter.getDirection()
        );

        List<String> filters = new ArrayList<>();

        Set<String> destinationNodeFilterNames = new HashSet<>();
        String sourceTypeFilterName = null;
        String destinationTypeFilterName = null;
        List<String> sourceFilterNames = new ArrayList<>();
        List<String> destinationFilterNames = new ArrayList<>();
        List<String> relationshipTypeFilterNames = new ArrayList<>();

        if (sourceType != null) {
            sourceTypeFilterName = "sourceType";
            // TODO: escape string value
            filters.add(String.format("%s as var(func: eq(<type>, \"%s\"))", sourceTypeFilterName, sourceType));
        }

        if (destinationType != null) {
            destinationTypeFilterName = "destinationType";
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

        // the destination node filter is the first filter that is being applied on the destination node
        // we can add multiple filters, they will combine as OR
        if (destinationTypeFilterName != null) {
            destinationNodeFilterNames.add(destinationTypeFilterName);
        }
        destinationNodeFilterNames.addAll(destinationFilterNames);
        destinationNodeFilterNames.addAll(relationshipTypeFilterNames);

        StringJoiner destinationNodeFilterJoiner = new StringJoiner(", ");
        destinationNodeFilterNames.stream().sorted().forEach(destinationNodeFilterJoiner::add);
        String destinationNodeFilter = destinationNodeFilterJoiner.toString();

        String filterConditions = getFilterConditions(
                sourceTypeFilterName, destinationTypeFilterName,
                sourceFilterNames, destinationFilterNames,
                relationshipTypeFilterNames, directedRelationshipTypes
        );

        StringJoiner relationshipsJoiner = new StringJoiner("\n    ");
        getRelationships(sourceTypeFilterName, sourceFilterNames, directedRelationshipTypes)
                .forEach(relationshipsJoiner::add);
        String relationships = relationshipsJoiner.toString();

        StringJoiner filterJoiner = new StringJoiner("\n  ");
        filters.forEach(filterJoiner::add);
        String filterExpressions = filterJoiner.toString();

        return String.format("query {\n"
                        + "  %s\n"
                        + "\n"
                        + "  result (func: uid(%s), first: %d, offset: %d) %s {\n"
                        + "    <urn>\n"
                        + "    %s\n"
                        + "  }\n"
                        + "}",
                filterExpressions,
                destinationNodeFilter,
                count, offset,
                filterConditions,
                relationships);
    }

    @Nonnull
    @Override
    public RelatedEntitiesResult findRelatedEntities(@Nullable String sourceType,
                                                     @Nonnull Filter sourceEntityFilter,
                                                     @Nullable String destinationType,
                                                     @Nonnull Filter destinationEntityFilter,
                                                     @Nonnull List<String> relationshipTypes,
                                                     @Nonnull RelationshipFilter relationshipFilter,
                                                     int offset,
                                                     int count) {
            if (relationshipTypes.isEmpty()) {
            return new RelatedEntitiesResult(offset, 0, 0, Collections.emptyList());
        }

        String query = getQueryForRelatedEntities(
                sourceType, sourceEntityFilter,
                destinationType, destinationEntityFilter,
                relationshipTypes, relationshipFilter,
                offset, count
        );

        System.out.println("Query: " + query);

        Request request = Request.newBuilder()
                .setQuery(query)
                .build();

        Response response = this._client.newReadOnlyTransaction().doRequest(request);
        String json = response.getJson().toStringUtf8();
        Map<String, Object> data = getDataFromResponseJson(json);

        List<RelatedEntity> entities = getRelatedEntitiesFromResponseData(data);
        int total = offset + entities.size();
        if (entities.size() == count) {
            // indicate that there might be more results
            total++;
        }
        return new RelatedEntitiesResult(offset, entities.size(), total, entities);
    }

    // Creates filter conditions from destination to source nodes
    protected static @Nonnull String getFilterConditions(@Nullable String sourceTypeFilterName,
                                                         @Nullable String destinationTypeFilterName,
                                                         @Nonnull List<String> sourceFilterNames,
                                                         @Nonnull List<String> destinationFilterNames,
                                                         @Nonnull List<String> relationshipTypeFilterNames,
                                                         @Nonnull List<String> relationshipTypes) {
        if (relationshipTypes.size() != relationshipTypeFilterNames.size()) {
            throw new IllegalArgumentException("relationshipTypeFilterNames and relationshipTypes "
                    + "must have same size: " + relationshipTypeFilterNames + " vs. " + relationshipTypes);
        }

        if (sourceTypeFilterName == null
                && destinationTypeFilterName == null
                && sourceFilterNames.isEmpty()
                && destinationFilterNames.isEmpty()
                && relationshipTypeFilterNames.isEmpty()) {
            return "";
        }

        StringJoiner andJoiner = new StringJoiner(" AND\n    ");
        if (destinationTypeFilterName != null) {
            andJoiner.add(String.format("uid(%s)", destinationTypeFilterName));
        }

        destinationFilterNames.forEach(filter -> andJoiner.add(String.format("uid(%s)", filter)));

        if (!relationshipTypes.isEmpty()) {
            StringJoiner orJoiner = new StringJoiner(" OR\n      ");
            IntStream.range(0, relationshipTypes.size()).forEach(idx -> orJoiner.add(getRelationshipCondition(
                    relationshipTypes.get(idx), relationshipTypeFilterNames.get(idx),
                    sourceTypeFilterName, sourceFilterNames
            )));
            String relationshipCondition = orJoiner.toString();
            andJoiner.add(String.format("(\n      %s\n    )", relationshipCondition));
        }

        String conditions = andJoiner.toString();
        return String.format("@filter(\n    %s\n  )", conditions);
    }

    protected static String getRelationshipCondition(@Nonnull String relationshipType,
                                                     @Nonnull String relationshipTypeFilterName,
                                                     @Nullable String objectFilterName,
                                                     @Nonnull List<String> destinationFilterNames) {
        StringJoiner andJoiner = new StringJoiner(" AND ");
        andJoiner.add(String.format("uid(%s)", relationshipTypeFilterName));
        if (objectFilterName != null) {
            andJoiner.add(String.format("uid_in(<%s>, uid(%s))", relationshipType, objectFilterName));
        }
        destinationFilterNames.forEach(filter -> andJoiner.add(String.format("uid_in(<%s>, uid(%s))", relationshipType, filter)));
        return andJoiner.toString();
    }


    // Creates filter conditions from destination to source nodes
    protected static @Nonnull List<String> getRelationships(@Nullable String sourceTypeFilterName,
                                                            @Nonnull List<String> sourceFilterNames,
                                                            @Nonnull List<String> relationshipTypes) {
        return relationshipTypes.stream().map(relationshipType -> {
            StringJoiner andJoiner = new StringJoiner(" AND ");
            if (sourceTypeFilterName != null) {
                andJoiner.add(String.format("uid(%s)", sourceTypeFilterName));
            }
            sourceFilterNames.forEach(filterName -> andJoiner.add(String.format("uid(%s)", filterName)));

            if (andJoiner.length() > 0) {
                return String.format("<%s> @filter( %s ) { <uid> }", relationshipType, andJoiner);
            } else {
                return String.format("<%s> { <uid> }", relationshipType);
            }
        }).collect(Collectors.toList());
    }

    protected static Map<String, Object> getDataFromResponseJson(String json) {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() { };
        try {
            return mapper.readValue(json, typeRef);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse response json: " + json.substring(0, 1000), e);
        }
    }

    protected static List<RelatedEntity> getRelatedEntitiesFromResponseData(Map<String, Object> data) {
        Object obj = data.get("result");
        if (!(obj instanceof List<?>)) {
            throw new IllegalArgumentException(
                    "The result from Dgraph did not contain a 'result' field, or that field is not a List"
            );
        }

        List<?> results = (List<?>) obj;
        return results.stream().flatMap(destinationObj -> {
            if (!(destinationObj instanceof Map)) {
                return Stream.empty();
            }

            Map<?, ?> destination = (Map<?, ?>) destinationObj;
            if (destination.containsKey("urn") && destination.get("urn") instanceof String) {
                String urn = (String) destination.get("urn");

                return destination.entrySet().stream()
                        .filter(entry -> !entry.getKey().equals("urn"))
                        .flatMap(entry -> {
                            Object relationshipObj = entry.getKey();
                            Object sourcesObj = entry.getValue();
                            if (!(relationshipObj instanceof String && sourcesObj instanceof List)) {
                                return Stream.empty();
                            }

                            String relationship = (String) relationshipObj;
                            List<?> sources = (List<?>) sourcesObj;

                            if (sources.size() == 0) {
                                return Stream.empty();
                            }

                            if (relationship.startsWith("~")) {
                                relationship = relationship.substring(1);
                            }

                            return Stream.of(relationship);
                        })
                        // for undirected we get duplicate relationships
                        .distinct()
                        .map(relationship -> new RelatedEntity(relationship, urn));
            }

            return Stream.empty();
        }).collect(Collectors.toList());
    }

    @Override
    public void removeNode(@Nonnull Urn urn) {
        String query = String.format("query {\n"
                + " node as var(func: eq(urn, \"%s\"))\n"
                + "}", urn);
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
        if (relationshipTypes.isEmpty()) {
            return;
        }

        RelationshipDirection direction = relationshipFilter.getDirection();

        if (direction == RelationshipDirection.OUTGOING || direction == RelationshipDirection.UNDIRECTED) {
            removeOutgoingEdgesFromNode(urn, relationshipTypes);
        }

        if (direction == RelationshipDirection.INCOMING || direction == RelationshipDirection.UNDIRECTED) {
            removeIncomingEdgesFromNode(urn, relationshipTypes);
        }
    }

    private void removeOutgoingEdgesFromNode(@Nonnull Urn urn,
                                             @Nonnull List<String> relationshipTypes) {
        // TODO: add escape for string values
        String query = String.format("query {\n"
                + "  node as var(func: eq(<urn>, \"%s\"))\n"
                + "}", urn);

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
                reverseEdges.add("<~" + relationshipTypes.get(idx) + "> { uids" + (idx + 1) + " as uid }")
        );
        String query = String.format("query {\n"
                + "  node as var(func: eq(<urn>, \"%s\"))\n"
                + "\n"
                + "  var(func: uid(node)) @normalize {\n"
                + "    %s\n"
                + "  }\n"
                + "}", urn, reverseEdges);

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
