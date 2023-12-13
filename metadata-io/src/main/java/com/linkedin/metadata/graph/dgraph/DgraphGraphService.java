package com.linkedin.metadata.graph.dgraph;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.NQuad;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Request;
import io.dgraph.DgraphProto.Response;
import io.dgraph.DgraphProto.Value;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class DgraphGraphService implements GraphService {

  // calls to Dgraph cluster will be retried if they throw retry-able exceptions
  // with a max number of attempts of 160 a call will finally fail after around 15 minutes
  private static final int MAX_ATTEMPTS = 160;

  private final @Nonnull DgraphExecutor _dgraph;
  private final @Nonnull LineageRegistry _lineageRegistry;

  private static final String URN_RELATIONSHIP_TYPE = "urn";
  private static final String TYPE_RELATIONSHIP_TYPE = "type";
  private static final String KEY_RELATIONSHIP_TYPE = "key";

  @Getter(lazy = true)
  // we want to defer initialization of schema (accessing Dgraph server) to the first time accessing
  // _schema
  private final DgraphSchema _schema = getSchema();

  public DgraphGraphService(
      @Nonnull LineageRegistry lineageRegistry, @Nonnull DgraphClient client) {
    _lineageRegistry = lineageRegistry;
    this._dgraph = new DgraphExecutor(client, MAX_ATTEMPTS);
  }

  protected @Nonnull DgraphSchema getSchema() {
    Response response =
        _dgraph.executeFunction(
            dgraphClient ->
                dgraphClient
                    .newReadOnlyTransaction()
                    .doRequest(Request.newBuilder().setQuery("schema { predicate }").build()));
    DgraphSchema schema = getSchema(response.getJson().toStringUtf8()).withDgraph(_dgraph);

    if (schema.isEmpty()) {
      Operation setSchema =
          Operation.newBuilder()
              .setSchema(
                  ""
                      + "<urn>: string @index(hash) @upsert .\n"
                      + "<type>: string @index(hash) .\n"
                      + "<key>: string @index(hash) .\n")
              .build();
      _dgraph.executeConsumer(dgraphClient -> dgraphClient.alter(setSchema));
    }

    return schema;
  }

  protected static @Nonnull DgraphSchema getSchema(@Nonnull String json) {
    Map<String, Object> data = getDataFromResponseJson(json);

    Object schemaObj = data.get("schema");
    if (!(schemaObj instanceof List<?>)) {
      log.info(
          "The result from Dgraph did not contain a 'schema' field, or that field is not a List");
      return DgraphSchema.empty();
    }

    List<?> schemaList = (List<?>) schemaObj;
    Set<String> fieldNames =
        schemaList.stream()
            .flatMap(
                fieldObj -> {
                  if (!(fieldObj instanceof Map)) {
                    return Stream.empty();
                  }

                  Map<?, ?> fieldMap = (Map<?, ?>) fieldObj;
                  if (!(fieldMap.containsKey("predicate")
                      && fieldMap.get("predicate") instanceof String)) {
                    return Stream.empty();
                  }

                  String fieldName = (String) fieldMap.get("predicate");
                  return Stream.of(fieldName);
                })
            .filter(f -> !f.startsWith("dgraph."))
            .collect(Collectors.toSet());

    Object typesObj = data.get("types");
    if (!(typesObj instanceof List<?>)) {
      log.info(
          "The result from Dgraph did not contain a 'types' field, or that field is not a List");
      return DgraphSchema.empty();
    }

    List<?> types = (List<?>) typesObj;
    Map<String, Set<String>> typeFields =
        types.stream()
            .flatMap(
                typeObj -> {
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

                  Set<String> fields =
                      fieldsList.stream()
                          .flatMap(
                              fieldObj -> {
                                if (!(fieldObj instanceof Map<?, ?>)) {
                                  return Stream.empty();
                                }

                                Map<?, ?> fieldMap = (Map<?, ?>) fieldObj;
                                if (!(fieldMap.containsKey("name")
                                    && fieldMap.get("name") instanceof String)) {
                                  return Stream.empty();
                                }

                                String fieldName = (String) fieldMap.get("name");
                                return Stream.of(fieldName);
                              })
                          .filter(f -> !f.startsWith("dgraph."))
                          .collect(Collectors.toSet());
                  return Stream.of(Pair.of(typeName, fields));
                })
            .filter(t -> !t.getKey().startsWith("dgraph."))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    return new DgraphSchema(fieldNames, typeFields);
  }

  @Override
  public LineageRegistry getLineageRegistry() {
    return _lineageRegistry;
  }

  @Override
  public void addEdge(Edge edge) {
    log.debug(
        String.format(
            "Adding Edge source: %s, destination: %s, type: %s",
            edge.getSource(), edge.getDestination(), edge.getRelationshipType()));

    // add the relationship type to the schema
    // TODO: translate edge name to allowed dgraph uris
    String sourceEntityType = getDgraphType(edge.getSource());
    String relationshipType = edge.getRelationshipType();
    get_schema()
        .ensureField(
            sourceEntityType,
            relationshipType,
            URN_RELATIONSHIP_TYPE,
            TYPE_RELATIONSHIP_TYPE,
            KEY_RELATIONSHIP_TYPE);

    // lookup the source and destination nodes
    // TODO: add escape for string values
    String query =
        String.format(
            "query {\n"
                + " src as var(func: eq(urn, \"%s\"))\n"
                + " dst as var(func: eq(urn, \"%s\"))\n"
                + "}",
            edge.getSource(), edge.getDestination());
    String srcVar = "uid(src)";
    String dstVar = "uid(dst)";

    // edge case: source and destination are same node
    if (edge.getSource().equals(edge.getDestination())) {
      query =
          String.format(
              "query {\n" + " node as var(func: eq(urn, \"%s\"))\n" + "}", edge.getSource());
      srcVar = "uid(node)";
      dstVar = "uid(node)";
    }

    // create source and destination nodes if they do not exist
    // and create the new edge between them
    // TODO: add escape for string values
    // TODO: translate edge name to allowed dgraph uris
    StringJoiner mutations = new StringJoiner("\n");
    mutations.add(
        String.format("%s <dgraph.type> \"%s\" .", srcVar, getDgraphType(edge.getSource())));
    mutations.add(String.format("%s <urn> \"%s\" .", srcVar, edge.getSource()));
    mutations.add(String.format("%s <type> \"%s\" .", srcVar, edge.getSource().getEntityType()));
    mutations.add(String.format("%s <key> \"%s\" .", srcVar, edge.getSource().getEntityKey()));
    if (!edge.getSource().equals(edge.getDestination())) {
      mutations.add(
          String.format("%s <dgraph.type> \"%s\" .", dstVar, getDgraphType(edge.getDestination())));
      mutations.add(String.format("%s <urn> \"%s\" .", dstVar, edge.getDestination()));
      mutations.add(
          String.format("%s <type> \"%s\" .", dstVar, edge.getDestination().getEntityType()));
      mutations.add(
          String.format("%s <key> \"%s\" .", dstVar, edge.getDestination().getEntityKey()));
    }
    mutations.add(String.format("%s <%s> %s .", srcVar, edge.getRelationshipType(), dstVar));

    log.debug("Query: " + query);
    log.debug("Mutations: " + mutations);

    // construct the upsert
    Mutation mutation =
        Mutation.newBuilder().setSetNquads(ByteString.copyFromUtf8(mutations.toString())).build();
    Request request =
        Request.newBuilder().setQuery(query).addMutations(mutation).setCommitNow(true).build();

    // run the request
    _dgraph.executeFunction(client -> client.newTransaction().doRequest(request));
  }

  private static @Nonnull String getDgraphType(@Nonnull Urn urn) {
    return urn.getNamespace() + ":" + urn.getEntityType();
  }

  // Returns reversed and directed relationship types:
  // <rel> returns <~rel> on outgoing and <rel> on incoming and both on undirected
  private static List<String> getDirectedRelationshipTypes(
      List<String> relationships, RelationshipDirection direction) {

    if (direction == RelationshipDirection.OUTGOING
        || direction == RelationshipDirection.UNDIRECTED) {
      List<String> outgoingRelationships =
          relationships.stream().map(type -> "~" + type).collect(Collectors.toList());

      if (direction == RelationshipDirection.OUTGOING) {
        return outgoingRelationships;
      } else {
        relationships = new ArrayList<>(relationships);
        relationships.addAll(outgoingRelationships);
      }
    }

    // we need to remove duplicates in order to not cause invalid queries in dgraph
    return new ArrayList<>(new LinkedHashSet(relationships));
  }

  protected static String getQueryForRelatedEntities(
      @Nullable List<String> sourceTypes,
      @Nonnull Filter sourceEntityFilter,
      @Nullable List<String> destinationTypes,
      @Nonnull Filter destinationEntityFilter,
      @Nonnull List<String> relationshipTypes,
      @Nonnull RelationshipFilter relationshipFilter,
      int offset,
      int count) {
    if (relationshipTypes.isEmpty()) {
      // we would have to construct a query that never returns any results
      // just do not call this method in the first place
      throw new IllegalArgumentException("The relationship types must not be empty");
    }

    if (sourceEntityFilter.hasCriteria() || destinationEntityFilter.hasCriteria()) {
      throw new IllegalArgumentException(
          "The DgraphGraphService does not support criteria in source or destination entity filter");
    }

    //noinspection ConstantConditions
    if (sourceEntityFilter.hasOr() && sourceEntityFilter.getOr().size() > 1
        || destinationEntityFilter.hasOr() && destinationEntityFilter.getOr().size() > 1) {
      throw new IllegalArgumentException(
          "The DgraphGraphService does not support multiple OR criteria in source or destination entity filter");
    }

    //noinspection ConstantConditions
    if (relationshipFilter.hasCriteria()
        || relationshipFilter.hasOr() && relationshipFilter.getOr().size() > 0) {
      throw new IllegalArgumentException(
          "The DgraphGraphService does not support any criteria for the relationship filter");
    }

    // We are not querying for <src> <relationship> <dest> and return <dest>
    // but we reverse the relationship and query for <dest> <~relationship> <src>
    // this guarantees there are no duplicates among the returned <dest>s
    final List<String> directedRelationshipTypes =
        getDirectedRelationshipTypes(relationshipTypes, relationshipFilter.getDirection());

    List<String> filters = new ArrayList<>();

    Set<String> destinationNodeFilterNames = new HashSet<>();
    String sourceTypeFilterName = null;
    String destinationTypeFilterName = null;
    List<String> sourceFilterNames = new ArrayList<>();
    List<String> destinationFilterNames = new ArrayList<>();
    List<String> relationshipTypeFilterNames = new ArrayList<>();

    if (sourceTypes != null && sourceTypes.size() > 0) {
      sourceTypeFilterName = "sourceType";
      // TODO: escape string value
      final StringJoiner joiner = new StringJoiner("\",\"", "[\"", "\"]");
      sourceTypes.forEach(type -> joiner.add(type));
      filters.add(
          String.format(
              "%s as var(func: eq(<type>, %s))", sourceTypeFilterName, joiner.toString()));
    }

    if (destinationTypes != null && destinationTypes.size() > 0) {
      destinationTypeFilterName = "destinationType";
      final StringJoiner joiner = new StringJoiner("\",\"", "[\"", "\"]");
      destinationTypes.forEach(type -> joiner.add(type));
      // TODO: escape string value
      filters.add(
          String.format(
              "%s as var(func: eq(<type>, %s))", destinationTypeFilterName, joiner.toString()));
    }

    //noinspection ConstantConditions
    if (sourceEntityFilter.hasOr() && sourceEntityFilter.getOr().size() == 1) {
      CriterionArray sourceCriteria = sourceEntityFilter.getOr().get(0).getAnd();
      IntStream.range(0, sourceCriteria.size())
          .forEach(
              idx -> {
                String sourceFilterName = "sourceFilter" + (idx + 1);
                sourceFilterNames.add(sourceFilterName);
                Criterion criterion = sourceCriteria.get(idx);
                // TODO: escape field name and string value
                filters.add(
                    String.format(
                        "%s as var(func: eq(<%s>, \"%s\"))",
                        sourceFilterName, criterion.getField(), criterion.getValue()));
              });
    }

    //noinspection ConstantConditions
    if (destinationEntityFilter.hasOr() && destinationEntityFilter.getOr().size() == 1) {
      CriterionArray destinationCriteria = destinationEntityFilter.getOr().get(0).getAnd();
      IntStream.range(0, destinationCriteria.size())
          .forEach(
              idx -> {
                String sourceFilterName = "destinationFilter" + (idx + 1);
                destinationFilterNames.add(sourceFilterName);
                Criterion criterion = destinationCriteria.get(idx);
                // TODO: escape field name and string value
                filters.add(
                    String.format(
                        "%s as var(func: eq(<%s>, \"%s\"))",
                        sourceFilterName, criterion.getField(), criterion.getValue()));
              });
    }

    IntStream.range(0, directedRelationshipTypes.size())
        .forEach(
            idx -> {
              String relationshipTypeFilterName = "relationshipType" + (idx + 1);
              relationshipTypeFilterNames.add(relationshipTypeFilterName);
              // TODO: escape string value
              filters.add(
                  String.format(
                      "%s as var(func: has(<%s>))",
                      relationshipTypeFilterName, directedRelationshipTypes.get(idx)));
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

    String filterConditions =
        getFilterConditions(
            sourceTypeFilterName, destinationTypeFilterName,
            sourceFilterNames, destinationFilterNames,
            relationshipTypeFilterNames, directedRelationshipTypes);

    StringJoiner relationshipsJoiner = new StringJoiner("\n    ");
    getRelationships(sourceTypeFilterName, sourceFilterNames, directedRelationshipTypes)
        .forEach(relationshipsJoiner::add);
    String relationships = relationshipsJoiner.toString();

    StringJoiner filterJoiner = new StringJoiner("\n  ");
    filters.forEach(filterJoiner::add);
    String filterExpressions = filterJoiner.toString();

    return String.format(
        "query {\n"
            + "  %s\n"
            + "\n"
            + "  result (func: uid(%s), first: %d, offset: %d) %s {\n"
            + "    <urn>\n"
            + "    %s\n"
            + "  }\n"
            + "}",
        filterExpressions, destinationNodeFilter, count, offset, filterConditions, relationships);
  }

  @Override
  public void upsertEdge(final Edge edge) {
    throw new UnsupportedOperationException(
        "Upsert edge not supported by Neo4JGraphService at this time.");
  }

  @Override
  public void removeEdge(final Edge edge) {
    throw new UnsupportedOperationException(
        "Remove edge not supported by DgraphGraphService at this time.");
  }

  @Nonnull
  @Override
  public RelatedEntitiesResult findRelatedEntities(
      @Nullable List<String> sourceTypes,
      @Nonnull Filter sourceEntityFilter,
      @Nullable List<String> destinationTypes,
      @Nonnull Filter destinationEntityFilter,
      @Nonnull List<String> relationshipTypes,
      @Nonnull RelationshipFilter relationshipFilter,
      int offset,
      int count) {

    if (sourceTypes != null && sourceTypes.isEmpty()
        || destinationTypes != null && destinationTypes.isEmpty()) {
      return new RelatedEntitiesResult(offset, 0, 0, Collections.emptyList());
    }
    if (relationshipTypes.isEmpty()
        || relationshipTypes.stream()
            .noneMatch(relationship -> get_schema().hasField(relationship))) {
      return new RelatedEntitiesResult(offset, 0, 0, Collections.emptyList());
    }

    String query =
        getQueryForRelatedEntities(
            sourceTypes,
            sourceEntityFilter,
            destinationTypes,
            destinationEntityFilter,
            relationshipTypes.stream().filter(get_schema()::hasField).collect(Collectors.toList()),
            relationshipFilter,
            offset,
            count);

    Request request = Request.newBuilder().setQuery(query).build();

    log.debug("Query: " + query);
    Response response =
        _dgraph.executeFunction(client -> client.newReadOnlyTransaction().doRequest(request));
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
  protected static @Nonnull String getFilterConditions(
      @Nullable String sourceTypeFilterName,
      @Nullable String destinationTypeFilterName,
      @Nonnull List<String> sourceFilterNames,
      @Nonnull List<String> destinationFilterNames,
      @Nonnull List<String> relationshipTypeFilterNames,
      @Nonnull List<String> relationshipTypes) {
    if (relationshipTypes.size() != relationshipTypeFilterNames.size()) {
      throw new IllegalArgumentException(
          "relationshipTypeFilterNames and relationshipTypes "
              + "must have same size: "
              + relationshipTypeFilterNames
              + " vs. "
              + relationshipTypes);
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
      IntStream.range(0, relationshipTypes.size())
          .forEach(
              idx ->
                  orJoiner.add(
                      getRelationshipCondition(
                          relationshipTypes.get(idx),
                          relationshipTypeFilterNames.get(idx),
                          sourceTypeFilterName,
                          sourceFilterNames)));
      String relationshipCondition = orJoiner.toString();
      andJoiner.add(String.format("(\n      %s\n    )", relationshipCondition));
    }

    String conditions = andJoiner.toString();
    return String.format("@filter(\n    %s\n  )", conditions);
  }

  protected static String getRelationshipCondition(
      @Nonnull String relationshipType,
      @Nonnull String relationshipTypeFilterName,
      @Nullable String objectFilterName,
      @Nonnull List<String> destinationFilterNames) {
    StringJoiner andJoiner = new StringJoiner(" AND ");
    andJoiner.add(String.format("uid(%s)", relationshipTypeFilterName));
    if (objectFilterName != null) {
      andJoiner.add(String.format("uid_in(<%s>, uid(%s))", relationshipType, objectFilterName));
    }
    destinationFilterNames.forEach(
        filter -> andJoiner.add(String.format("uid_in(<%s>, uid(%s))", relationshipType, filter)));
    return andJoiner.toString();
  }

  // Creates filter conditions from destination to source nodes
  protected static @Nonnull List<String> getRelationships(
      @Nullable String sourceTypeFilterName,
      @Nonnull List<String> sourceFilterNames,
      @Nonnull List<String> relationshipTypes) {
    return relationshipTypes.stream()
        .map(
            relationshipType -> {
              StringJoiner andJoiner = new StringJoiner(" AND ");
              if (sourceTypeFilterName != null) {
                andJoiner.add(String.format("uid(%s)", sourceTypeFilterName));
              }
              sourceFilterNames.forEach(
                  filterName -> andJoiner.add(String.format("uid(%s)", filterName)));

              if (andJoiner.length() > 0) {
                return String.format("<%s> @filter( %s ) { <uid> }", relationshipType, andJoiner);
              } else {
                return String.format("<%s> { <uid> }", relationshipType);
              }
            })
        .collect(Collectors.toList());
  }

  protected static Map<String, Object> getDataFromResponseJson(String json) {
    ObjectMapper mapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    mapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    TypeReference<HashMap<String, Object>> typeRef =
        new TypeReference<HashMap<String, Object>>() {};
    try {
      return mapper.readValue(json, typeRef);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse response json: " + json.substring(0, 1000), e);
    }
  }

  protected static List<RelatedEntity> getRelatedEntitiesFromResponseData(
      Map<String, Object> data) {
    Object obj = data.get("result");
    if (!(obj instanceof List<?>)) {
      throw new IllegalArgumentException(
          "The result from Dgraph did not contain a 'result' field, or that field is not a List");
    }

    List<?> results = (List<?>) obj;
    return results.stream()
        .flatMap(
            destinationObj -> {
              if (!(destinationObj instanceof Map)) {
                return Stream.empty();
              }

              Map<?, ?> destination = (Map<?, ?>) destinationObj;
              if (destination.containsKey("urn") && destination.get("urn") instanceof String) {
                String urn = (String) destination.get("urn");

                return destination.entrySet().stream()
                    .filter(entry -> !entry.getKey().equals("urn"))
                    .flatMap(
                        entry -> {
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
            })
        .collect(Collectors.toList());
  }

  @Override
  public void removeNode(@Nonnull Urn urn) {
    String query = String.format("query {\n" + " node as var(func: eq(urn, \"%s\"))\n" + "}", urn);
    String deletion = "uid(node) * * .";

    log.debug("Query: " + query);
    log.debug("Delete: " + deletion);

    Mutation mutation =
        Mutation.newBuilder().setDelNquads(ByteString.copyFromUtf8(deletion)).build();
    Request request =
        Request.newBuilder().setQuery(query).addMutations(mutation).setCommitNow(true).build();

    _dgraph.executeConsumer(client -> client.newTransaction().doRequest(request));
  }

  @Override
  public void removeEdgesFromNode(
      @Nonnull Urn urn,
      @Nonnull List<String> relationshipTypes,
      @Nonnull RelationshipFilter relationshipFilter) {
    if (relationshipTypes.isEmpty()) {
      return;
    }

    RelationshipDirection direction = relationshipFilter.getDirection();

    if (direction == RelationshipDirection.OUTGOING
        || direction == RelationshipDirection.UNDIRECTED) {
      removeOutgoingEdgesFromNode(urn, relationshipTypes);
    }

    if (direction == RelationshipDirection.INCOMING
        || direction == RelationshipDirection.UNDIRECTED) {
      removeIncomingEdgesFromNode(urn, relationshipTypes);
    }
  }

  private void removeOutgoingEdgesFromNode(
      @Nonnull Urn urn, @Nonnull List<String> relationshipTypes) {
    // TODO: add escape for string values
    String query =
        String.format("query {\n" + "  node as var(func: eq(<urn>, \"%s\"))\n" + "}", urn);

    Value star = Value.newBuilder().setDefaultVal("_STAR_ALL").build();
    List<NQuad> deletions =
        relationshipTypes.stream()
            .map(
                relationshipType ->
                    NQuad.newBuilder()
                        .setSubject("uid(node)")
                        .setPredicate(relationshipType)
                        .setObjectValue(star)
                        .build())
            .collect(Collectors.toList());

    log.debug("Query: " + query);
    log.debug("Deletions: " + deletions);

    Mutation mutation = Mutation.newBuilder().addAllDel(deletions).build();
    Request request =
        Request.newBuilder().setQuery(query).addMutations(mutation).setCommitNow(true).build();

    _dgraph.executeConsumer(client -> client.newTransaction().doRequest(request));
  }

  private void removeIncomingEdgesFromNode(
      @Nonnull Urn urn, @Nonnull List<String> relationshipTypes) {
    // TODO: add escape for string values
    StringJoiner reverseEdges = new StringJoiner("\n    ");
    IntStream.range(0, relationshipTypes.size())
        .forEach(
            idx ->
                reverseEdges.add(
                    "<~" + relationshipTypes.get(idx) + "> { uids" + (idx + 1) + " as uid }"));
    String query =
        String.format(
            "query {\n"
                + "  node as var(func: eq(<urn>, \"%s\"))\n"
                + "\n"
                + "  var(func: uid(node)) @normalize {\n"
                + "    %s\n"
                + "  }\n"
                + "}",
            urn, reverseEdges);

    StringJoiner deletions = new StringJoiner("\n");
    IntStream.range(0, relationshipTypes.size())
        .forEach(
            idx ->
                deletions.add(
                    "uid(uids" + (idx + 1) + ") <" + relationshipTypes.get(idx) + "> uid(node) ."));

    log.debug("Query: " + query);
    log.debug("Deletions: " + deletions);

    Mutation mutation =
        Mutation.newBuilder().setDelNquads(ByteString.copyFromUtf8(deletions.toString())).build();
    Request request =
        Request.newBuilder().setQuery(query).addMutations(mutation).setCommitNow(true).build();

    _dgraph.executeConsumer(client -> client.newTransaction().doRequest(request));
  }

  @Override
  public void configure() {}

  @Override
  public void clear() {
    log.debug("dropping Dgraph data");

    Operation dropAll = Operation.newBuilder().setDropOp(Operation.DropOp.ALL).build();
    _dgraph.executeConsumer(client -> client.alter(dropAll));

    // drop schema cache
    get_schema().clear();

    // setup urn, type and key relationships
    getSchema();
  }
}
