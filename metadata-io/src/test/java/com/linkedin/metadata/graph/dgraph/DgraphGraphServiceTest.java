package com.linkedin.metadata.graph.dgraph;

import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.GraphServiceTestBase;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.linkedin.metadata.DockerTestUtils.checkContainerEngine;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
@Slf4j
public class DgraphGraphServiceTest extends GraphServiceTestBase {

    private ManagedChannel _channel;
    private DgraphGraphService _service;
    private DgraphContainer _container;

    @Override
    protected Duration getTestConcurrentOpTimeout() {
        return Duration.ofMinutes(5);
    }

    @BeforeTest
    public void setup() {
        _container = new DgraphContainer(DgraphContainer.DEFAULT_IMAGE_NAME.withTag("v21.03.0"))
                .withTmpFs(Collections.singletonMap("/dgraph", "rw,noexec,nosuid,size=1g"))
                .withStartupTimeout(Duration.ofMinutes(1))
                .withStartupAttempts(3);
        checkContainerEngine(_container.getDockerClient());
        _container.start();
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(log);
        _container.followOutput(logConsumer);
    }

    @BeforeMethod
    public void connect() {
        LineageRegistry lineageRegistry = new LineageRegistry(SnapshotEntityRegistry.getInstance());
        _channel = ManagedChannelBuilder
                .forAddress(_container.getHost(), _container.getGrpcPort())
                .usePlaintext()
                .build();

        // https://discuss.dgraph.io/t/dgraph-java-client-setting-deadlines-per-call/3056
        ClientInterceptor timeoutInterceptor = new ClientInterceptor() {
            @Override
            public <REQ, RESP> ClientCall<REQ, RESP> interceptCall(
                    MethodDescriptor<REQ, RESP> method, CallOptions callOptions, Channel next) {
                return next.newCall(method, callOptions.withDeadlineAfter(30, TimeUnit.SECONDS));
            }
        };

        DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(_channel).withInterceptors(timeoutInterceptor);
        _service = new DgraphGraphService(lineageRegistry, new DgraphClient(stub));
    }

    @AfterMethod
    public void disconnect() throws InterruptedException {
        try {
            _channel.shutdownNow();
            _channel.awaitTermination(10, TimeUnit.SECONDS);
        } finally {
            _channel = null;
            _service = null;
        }
    }

    @AfterTest
    public void tearDown() {
        _container.stop();
    }

    @Nonnull
    @Override
    protected GraphService getGraphService() {
        _service.clear();
        return _service;
    }

    @Override
    protected void syncAfterWrite() { }

    @Test
    public void testGetSchema() {
        DgraphSchema schema = DgraphGraphService.getSchema("{\n"
                + "    \"schema\": [\n"
                + "      {\n"
                + "        \"predicate\": \"PredOne\"\n"
                + "      },\n"
                + "      {\n"
                + "        \"predicate\": \"PredTwo\"\n"
                + "      },\n"
                + "      {\n"
                + "        \"predicate\": \"dgraph.type\"\n"
                + "      }\n"
                + "    ],\n"
                + "    \"types\": [\n"
                + "      {\n"
                + "        \"fields\": [\n"
                + "          {\n"
                + "            \"name\": \"dgraph.type\"\n"
                + "          }\n"
                + "        ],\n"
                + "        \"name\": \"dgraph.meta\"\n"
                + "      },\n"
                + "      {\n"
                + "        \"fields\": [\n"
                + "          {\n"
                + "            \"name\": \"PredOne\"\n"
                + "          },\n"
                + "          {\n"
                + "            \"name\": \"PredTwo\"\n"
                + "          }\n"
                + "        ],\n"
                + "        \"name\": \"ns:typeOne\"\n"
                + "      },\n"
                + "      {\n"
                + "        \"fields\": [\n"
                + "          {\n"
                + "            \"name\": \"PredTwo\"\n"
                + "          }\n"
                + "        ],\n"
                + "        \"name\": \"ns:typeTwo\"\n"
                + "      }\n"
                + "    ]\n"
                + "  }");
        assertEquals(schema.getFields(), new HashSet<>(Arrays.asList("PredOne", "PredTwo")));

        assertEquals(schema.getTypes(), new HashMap<String, Set<String>>() {{
            put("ns:typeOne", new HashSet<>(Arrays.asList("PredOne", "PredTwo")));
            put("ns:typeTwo", new HashSet<>(Arrays.asList("PredTwo")));
        }});

        assertEquals(schema.getFields("ns:typeOne"), new HashSet<>(Arrays.asList("PredOne", "PredTwo")));
        assertEquals(schema.getFields("ns:typeTwo"), new HashSet<>(Arrays.asList("PredTwo")));
        assertEquals(schema.getFields("ns:unknown"), Collections.emptySet());

        schema.ensureField("newType", "newField");
        assertEquals(schema.getFields(), new HashSet<>(Arrays.asList("PredOne", "PredTwo", "newField")));
        assertEquals(schema.getTypes(), new HashMap<String, Set<String>>() {{
            put("ns:typeOne", new HashSet<>(Arrays.asList("PredOne", "PredTwo")));
            put("ns:typeTwo", new HashSet<>(Arrays.asList("PredTwo")));
            put("newType", new HashSet<>(Arrays.asList("newField")));
        }});

        schema.ensureField("ns:typeOne", "otherField");
        assertEquals(schema.getFields(), new HashSet<>(Arrays.asList("PredOne", "PredTwo", "newField", "otherField")));
        assertEquals(schema.getTypes(), new HashMap<String, Set<String>>() {{
            put("ns:typeOne", new HashSet<>(Arrays.asList("PredOne", "PredTwo", "otherField")));
            put("ns:typeTwo", new HashSet<>(Arrays.asList("PredTwo")));
            put("newType", new HashSet<>(Arrays.asList("newField")));
        }});

        schema.ensureField("ns:typeTwo", "PredTwo");
        assertEquals(schema.getFields(), new HashSet<>(Arrays.asList("PredOne", "PredTwo", "newField", "otherField")));
        assertEquals(schema.getTypes(), new HashMap<String, Set<String>>() {{
            put("ns:typeOne", new HashSet<>(Arrays.asList("PredOne", "PredTwo", "otherField")));
            put("ns:typeTwo", new HashSet<>(Arrays.asList("PredTwo")));
            put("newType", new HashSet<>(Arrays.asList("newField")));
        }});
    }

    @Test
    public void testGetSchemaIncomplete() {
        DgraphSchema schemaWithNonListTypes = DgraphGraphService.getSchema("{\n"
                + "    \"schema\": [\n"
                + "      {\n"
                + "        \"predicate\": \"PredOne\"\n"
                + "      },\n"
                + "      {\n"
                + "        \"predicate\": \"PredTwo\"\n"
                + "      },\n"
                + "      {\n"
                + "        \"predicate\": \"dgraph.type\"\n"
                + "      }\n"
                + "    ],\n"
                + "    \"types\": \"not a list\"\n"
                + "  }");
        assertTrue(schemaWithNonListTypes.isEmpty(), "Should be empty if type field is not a list");

        DgraphSchema schemaWithoutTypes = DgraphGraphService.getSchema("{\n"
                + "    \"schema\": [\n"
                + "      {\n"
                + "        \"predicate\": \"PredOne\"\n"
                + "      },\n"
                + "      {\n"
                + "        \"predicate\": \"PredTwo\"\n"
                + "      },\n"
                + "      {\n"
                + "        \"predicate\": \"dgraph.type\"\n"
                + "      }\n"
                + "    ]"
                + "  }");
        assertTrue(schemaWithoutTypes.isEmpty(), "Should be empty if no type field exists");

        DgraphSchema schemaWithNonListSchema = DgraphGraphService.getSchema("{\n"
                + "    \"schema\": \"not a list\""
                + "  }");
        assertTrue(schemaWithNonListSchema.isEmpty(), "Should be empty if schema field is not a list");

        DgraphSchema schemaWithoutSchema = DgraphGraphService.getSchema("{ }");
        assertTrue(schemaWithoutSchema.isEmpty(), "Should be empty if no schema field exists");
    }

    @Test
    public void testGetSchemaDgraph() {
        // TODO: test that dgraph schema gets altered
    }

    @Test
    public void testGetFilterConditions() {
        // no filters
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        null,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList()),
                ""
        );

        // source type not supported without restricting relationship types
        // there must be as many relation type filter names as there are relationships
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        "sourceTypeFilter",
                        null,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Arrays.asList("RelationshipTypeFilter"),
                        Arrays.asList("relationship")),
                "@filter(\n"
                        + "    (\n"
                        + "      uid(RelationshipTypeFilter) AND uid_in(<relationship>, uid(sourceTypeFilter))\n"
                        + "    )\n"
                        + "  )"
        );

        // destination type
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        "destinationTypeFilter",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList()),
                "@filter(\n"
                        + "    uid(destinationTypeFilter)\n"
                        + "  )"
        );

        // source filter not supported without restricting relationship types
        // there must be as many relation type filter names as there are relationships
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        null,
                        Arrays.asList("sourceFilter"),
                        Collections.emptyList(),
                        Arrays.asList("RelationshipTypeFilter"),
                        Arrays.asList("relationship")),
                "@filter(\n"
                        + "    (\n"
                        + "      uid(RelationshipTypeFilter) AND uid_in(<relationship>, uid(sourceFilter))\n"
                        + "    )\n"
                        + "  )"
        );
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        null,
                        Arrays.asList("sourceFilter1", "sourceFilter2"),
                        Collections.emptyList(),
                        Arrays.asList("RelationshipTypeFilter"),
                        Arrays.asList("relationship")),
                "@filter(\n"
                        + "    (\n"
                        + "      uid(RelationshipTypeFilter) AND uid_in(<relationship>, uid(sourceFilter1)) AND "
                        + "uid_in(<relationship>, uid(sourceFilter2))\n"
                        + "    )\n"
                        + "  )"
        );
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        null,
                        Arrays.asList("sourceFilter1", "sourceFilter2"),
                        Collections.emptyList(),
                        Arrays.asList("RelationshipTypeFilter1", "RelationshipTypeFilter2"),
                        Arrays.asList("relationship1", "relationship2")),
                "@filter(\n"
                        + "    (\n"
                        + "      uid(RelationshipTypeFilter1) AND uid_in(<relationship1>, uid(sourceFilter1)) AND "
                        + "uid_in(<relationship1>, uid(sourceFilter2)) OR\n"
                        + "      uid(RelationshipTypeFilter2) AND uid_in(<relationship2>, uid(sourceFilter1)) AND "
                        + "uid_in(<relationship2>, uid(sourceFilter2))\n"
                        + "    )\n"
                        + "  )"
        );

        // destination filters
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        null,
                        Collections.emptyList(),
                        Arrays.asList("destinationFilter"),
                        Collections.emptyList(),
                        Collections.emptyList()),
                "@filter(\n"
                        + "    uid(destinationFilter)\n"
                        + "  )"
        );
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        null,
                        Collections.emptyList(),
                        Arrays.asList("destinationFilter1", "destinationFilter2"),
                        Collections.emptyList(),
                        Collections.emptyList()),
                "@filter(\n"
                        + "    uid(destinationFilter1) AND\n"
                        + "    uid(destinationFilter2)\n"
                        + "  )"
        );

        // relationship type filters require relationship types
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        null,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Arrays.asList("relationshipTypeFilter1", "relationshipTypeFilter2"),
                        Arrays.asList("relationship1", "relationship2")),
                "@filter(\n"
                        + "    (\n"
                        + "      uid(relationshipTypeFilter1) OR\n"
                        + "      uid(relationshipTypeFilter2)\n"
                        + "    )\n"
                        + "  )"
        );

        // all filters at once
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        "sourceTypeFilter",
                        "destinationTypeFilter",
                        Arrays.asList("sourceFilter1", "sourceFilter2"),
                        Arrays.asList("destinationFilter1", "destinationFilter2"),
                        Arrays.asList("relationshipTypeFilter1", "relationshipTypeFilter2"),
                        Arrays.asList("relationship1", "relationship2")),
                "@filter(\n"
                        + "    uid(destinationTypeFilter) AND\n"
                        + "    uid(destinationFilter1) AND\n"
                        + "    uid(destinationFilter2) AND\n"
                        + "    (\n"
                        + "      uid(relationshipTypeFilter1) AND uid_in(<relationship1>, uid(sourceTypeFilter)) AND "
                        + "uid_in(<relationship1>, uid(sourceFilter1)) AND uid_in(<relationship1>, uid(sourceFilter2)) OR\n"
                        + "      uid(relationshipTypeFilter2) AND uid_in(<relationship2>, uid(sourceTypeFilter)) AND "
                        + "uid_in(<relationship2>, uid(sourceFilter1)) AND uid_in(<relationship2>, uid(sourceFilter2))\n"
                        + "    )\n"
                        + "  )"
        );

        // TODO: check getFilterConditions throws an exception when relationshipTypes and
        //       relationshipTypeFilterNames do not have the same size
    }

    @Test
    public void testGetRelationships() {
        // no relationships
        assertEquals(
                DgraphGraphService.getRelationships(
                        null,
                        Collections.emptyList(),
                        Collections.emptyList()),
                Collections.emptyList()
        );

        // one relationship but no filters
        assertEquals(
                DgraphGraphService.getRelationships(
                        null,
                        Collections.emptyList(),
                        Arrays.asList("relationship")
                ),
                Arrays.asList("<relationship> { <uid> }")
        );

        // more relationship and source type filter
        assertEquals(
                DgraphGraphService.getRelationships(
                        "sourceTypeFilter",
                        Collections.emptyList(),
                        Arrays.asList("relationship1", "~relationship2")
                ),
                Arrays.asList(
                        "<relationship1> @filter( uid(sourceTypeFilter) ) { <uid> }",
                        "<~relationship2> @filter( uid(sourceTypeFilter) ) { <uid> }"
                )
        );

        // more relationship, source type and source filters
        assertEquals(
                DgraphGraphService.getRelationships(
                        "sourceTypeFilter",
                        Arrays.asList("sourceFilter1", "sourceFilter2"),
                        Arrays.asList("relationship1", "~relationship2")
                ),
                Arrays.asList(
                        "<relationship1> @filter( uid(sourceTypeFilter) AND uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }",
                        "<~relationship2> @filter( uid(sourceTypeFilter) AND uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }"
                )
        );

        // more relationship and only source filters
        assertEquals(
                DgraphGraphService.getRelationships(
                        null,
                        Arrays.asList("sourceFilter1", "sourceFilter2"),
                        Arrays.asList("relationship1", "~relationship2", "relationship3")
                ),
                Arrays.asList(
                        "<relationship1> @filter( uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }",
                        "<~relationship2> @filter( uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }",
                        "<relationship3> @filter( uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }"
                )
        );

        // two relationship and only one source filter
        assertEquals(
                DgraphGraphService.getRelationships(
                        null,
                        Arrays.asList("sourceFilter"),
                        Arrays.asList("~relationship1", "~relationship2")
                ),
                Arrays.asList(
                        "<~relationship1> @filter( uid(sourceFilter) ) { <uid> }",
                        "<~relationship2> @filter( uid(sourceFilter) ) { <uid> }"
                )
        );
    }

    @Test
    public void testGetRelationshipCondition() {
        assertEquals(
                DgraphGraphService.getRelationshipCondition(
                        "relationship",
                        "relationshipFilter",
                        null,
                        Collections.emptyList()),
                "uid(relationshipFilter)"
        );

        assertEquals(
                DgraphGraphService.getRelationshipCondition(
                        "relationship",
                        "relationshipFilter",
                        "destinationTypeFilter",
                        Collections.emptyList()),
                "uid(relationshipFilter) AND uid_in(<relationship>, uid(destinationTypeFilter))"
        );

        assertEquals(
                DgraphGraphService.getRelationshipCondition(
                        "relationship",
                        "relationshipFilter",
                        "destinationTypeFilter",
                        Arrays.asList("destinationFilter")),
                "uid(relationshipFilter) AND uid_in(<relationship>, uid(destinationTypeFilter)) AND "
                        + "uid_in(<relationship>, uid(destinationFilter))"
        );

        assertEquals(
                DgraphGraphService.getRelationshipCondition(
                        "relationship",
                        "relationshipFilter",
                        "destinationTypeFilter",
                        Arrays.asList("destinationFilter1", "destinationFilter2")),
                "uid(relationshipFilter) AND uid_in(<relationship>, uid(destinationTypeFilter)) AND "
                        + "uid_in(<relationship>, uid(destinationFilter1)) AND uid_in(<relationship>, uid(destinationFilter2))"
        );

        assertEquals(
                DgraphGraphService.getRelationshipCondition(
                        "relationship",
                        "relationshipFilter",
                        null,
                        Arrays.asList("destinationFilter1", "destinationFilter2")),
                "uid(relationshipFilter) AND uid_in(<relationship>, uid(destinationFilter1)) AND "
                        + "uid_in(<relationship>, uid(destinationFilter2))"
        );
    }

    @Test
    public void testGetQueryForRelatedEntitiesOutgoing() {
        doTestGetQueryForRelatedEntitiesDirection(RelationshipDirection.OUTGOING,
                "query {\n"
                        + "  sourceType as var(func: eq(<type>, \"sourceType\"))\n"
                        + "  destinationType as var(func: eq(<type>, \"destinationType\"))\n"
                        + "  sourceFilter1 as var(func: eq(<urn>, \"urn:ns:type:source-key\"))\n"
                        + "  sourceFilter2 as var(func: eq(<key>, \"source-key\"))\n"
                        + "  destinationFilter1 as var(func: eq(<urn>, \"urn:ns:type:dest-key\"))\n"
                        + "  destinationFilter2 as var(func: eq(<key>, \"dest-key\"))\n"
                        + "  relationshipType1 as var(func: has(<~relationship1>))\n"
                        + "  relationshipType2 as var(func: has(<~relationship2>))\n"
                        + "\n"
                        + "  result (func: uid(destinationFilter1, destinationFilter2, destinationType, relationshipType1, relationshipType2), "
                        + "first: 100, offset: 0) @filter(\n"
                        + "    uid(destinationType) AND\n"
                        + "    uid(destinationFilter1) AND\n"
                        + "    uid(destinationFilter2) AND\n"
                        + "    (\n"
                        + "      uid(relationshipType1) AND uid_in(<~relationship1>, uid(sourceType)) AND "
                        + "uid_in(<~relationship1>, uid(sourceFilter1)) AND uid_in(<~relationship1>, uid(sourceFilter2)) OR\n"
                        + "      uid(relationshipType2) AND uid_in(<~relationship2>, uid(sourceType)) AND "
                        + "uid_in(<~relationship2>, uid(sourceFilter1)) AND uid_in(<~relationship2>, uid(sourceFilter2))\n"
                        + "    )\n"
                        + "  ) {\n"
                        + "    <urn>\n"
                        + "    <~relationship1> @filter( uid(sourceType) AND uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }\n"
                        + "    <~relationship2> @filter( uid(sourceType) AND uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }\n"
                        + "  }\n"
                        + "}"
        );
    }

    @Test
    public void testGetQueryForRelatedEntitiesIncoming() {
        doTestGetQueryForRelatedEntitiesDirection(RelationshipDirection.INCOMING,
                "query {\n"
                        + "  sourceType as var(func: eq(<type>, \"sourceType\"))\n"
                        + "  destinationType as var(func: eq(<type>, \"destinationType\"))\n"
                        + "  sourceFilter1 as var(func: eq(<urn>, \"urn:ns:type:source-key\"))\n"
                        + "  sourceFilter2 as var(func: eq(<key>, \"source-key\"))\n"
                        + "  destinationFilter1 as var(func: eq(<urn>, \"urn:ns:type:dest-key\"))\n"
                        + "  destinationFilter2 as var(func: eq(<key>, \"dest-key\"))\n"
                        + "  relationshipType1 as var(func: has(<relationship1>))\n"
                        + "  relationshipType2 as var(func: has(<relationship2>))\n"
                        + "\n"
                        + "  result (func: uid(destinationFilter1, destinationFilter2, destinationType, relationshipType1, relationshipType2), "
                        + "first: 100, offset: 0) @filter(\n"
                        + "    uid(destinationType) AND\n"
                        + "    uid(destinationFilter1) AND\n"
                        + "    uid(destinationFilter2) AND\n"
                        + "    (\n"
                        + "      uid(relationshipType1) AND uid_in(<relationship1>, uid(sourceType)) AND "
                        + "uid_in(<relationship1>, uid(sourceFilter1)) AND uid_in(<relationship1>, uid(sourceFilter2)) OR\n"
                        + "      uid(relationshipType2) AND uid_in(<relationship2>, uid(sourceType)) AND "
                        + "uid_in(<relationship2>, uid(sourceFilter1)) AND uid_in(<relationship2>, uid(sourceFilter2))\n"
                        + "    )\n"
                        + "  ) {\n"
                        + "    <urn>\n"
                        + "    <relationship1> @filter( uid(sourceType) AND uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }\n"
                        + "    <relationship2> @filter( uid(sourceType) AND uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }\n"
                        + "  }\n"
                        + "}"
        );
    }

    @Test
    public void testGetQueryForRelatedEntitiesUndirected() {
        doTestGetQueryForRelatedEntitiesDirection(RelationshipDirection.UNDIRECTED,
                "query {\n"
                        + "  sourceType as var(func: eq(<type>, \"sourceType\"))\n"
                        + "  destinationType as var(func: eq(<type>, \"destinationType\"))\n"
                        + "  sourceFilter1 as var(func: eq(<urn>, \"urn:ns:type:source-key\"))\n"
                        + "  sourceFilter2 as var(func: eq(<key>, \"source-key\"))\n"
                        + "  destinationFilter1 as var(func: eq(<urn>, \"urn:ns:type:dest-key\"))\n"
                        + "  destinationFilter2 as var(func: eq(<key>, \"dest-key\"))\n"
                        + "  relationshipType1 as var(func: has(<relationship1>))\n"
                        + "  relationshipType2 as var(func: has(<relationship2>))\n"
                        + "  relationshipType3 as var(func: has(<~relationship1>))\n"
                        + "  relationshipType4 as var(func: has(<~relationship2>))\n"
                        + "\n"
                        + "  result (func: uid(destinationFilter1, destinationFilter2, destinationType, "
                        + "relationshipType1, relationshipType2, relationshipType3, relationshipType4), first: 100, offset: 0) @filter(\n"
                        + "    uid(destinationType) AND\n"
                        + "    uid(destinationFilter1) AND\n"
                        + "    uid(destinationFilter2) AND\n"
                        + "    (\n"
                        + "      uid(relationshipType1) AND uid_in(<relationship1>, uid(sourceType)) AND "
                        + "uid_in(<relationship1>, uid(sourceFilter1)) AND uid_in(<relationship1>, uid(sourceFilter2)) OR\n"
                        + "      uid(relationshipType2) AND uid_in(<relationship2>, uid(sourceType)) AND "
                        + "uid_in(<relationship2>, uid(sourceFilter1)) AND uid_in(<relationship2>, uid(sourceFilter2)) OR\n"
                        + "      uid(relationshipType3) AND uid_in(<~relationship1>, uid(sourceType)) AND "
                        + "uid_in(<~relationship1>, uid(sourceFilter1)) AND uid_in(<~relationship1>, uid(sourceFilter2)) OR\n"
                        + "      uid(relationshipType4) AND uid_in(<~relationship2>, uid(sourceType)) AND "
                        + "uid_in(<~relationship2>, uid(sourceFilter1)) AND uid_in(<~relationship2>, uid(sourceFilter2))\n"
                        + "    )\n"
                        + "  ) {\n"
                        + "    <urn>\n"
                        + "    <relationship1> @filter( uid(sourceType) AND uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }\n"
                        + "    <relationship2> @filter( uid(sourceType) AND uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }\n"
                        + "    <~relationship1> @filter( uid(sourceType) AND uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }\n"
                        + "    <~relationship2> @filter( uid(sourceType) AND uid(sourceFilter1) AND uid(sourceFilter2) ) { <uid> }\n"
                        + "  }\n"
                        + "}"
        );
    }

    private void doTestGetQueryForRelatedEntitiesDirection(@Nonnull RelationshipDirection direction, @Nonnull String expectedQuery) {
        assertEquals(
                DgraphGraphService.getQueryForRelatedEntities(
                        "sourceType",
                        newFilter(new HashMap<String, String>() {{
                            put("urn", "urn:ns:type:source-key");
                            put("key", "source-key");
                        }}),
                        "destinationType",
                        newFilter(new HashMap<String, String>() {{
                            put("urn", "urn:ns:type:dest-key");
                            put("key", "dest-key");
                        }}),
                        Arrays.asList("relationship1", "relationship2"),
                        newRelationshipFilter(EMPTY_FILTER, direction),
                        0, 100
                ),
                expectedQuery
        );
    }

    @Test
    public void testGetDestinationUrnsFromResponseData() {
        // no results
        assertEquals(
                DgraphGraphService.getRelatedEntitiesFromResponseData(
                        new HashMap<String, Object>() {{
                            put("result", Collections.emptyList());
                        }}
                ),
                Collections.emptyList()
        );

        // one result and one relationship with two sources
        assertEquals(
                DgraphGraphService.getRelatedEntitiesFromResponseData(
                        new HashMap<String, Object>() {{
                            put("result", Arrays.asList(
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key");
                                        put("~pred", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("uid", "0x1");
                                                }},
                                                new HashMap<String, Object>() {{
                                                    put("uid", "0x2");
                                                }}
                                        ));
                                    }}
                            ));
                        }}
                ),
                Arrays.asList(new RelatedEntity("pred", "urn:ns:type:dest-key"))
        );

        // multiple results and one relationship
        assertEquals(
                DgraphGraphService.getRelatedEntitiesFromResponseData(
                        new HashMap<String, Object>() {{
                            put("result", Arrays.asList(
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key-1");
                                        put("~pred", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("uid", "0x1");
                                                }},
                                                new HashMap<String, Object>() {{
                                                    put("uid", "0x2");
                                                }}
                                        ));
                                    }},
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key-2");
                                        put("~pred", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("uid", "0x2");
                                                }}
                                        ));
                                    }}
                            ));
                        }}
                ),
                Arrays.asList(
                        new RelatedEntity("pred", "urn:ns:type:dest-key-1"),
                        new RelatedEntity("pred", "urn:ns:type:dest-key-2")
                )
        );

        // multiple results and relationships
        assertEqualsAnyOrder(
                DgraphGraphService.getRelatedEntitiesFromResponseData(
                        new HashMap<String, Object>() {{
                            put("result", Arrays.asList(
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key-1");
                                        put("~pred1", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("uid", "0x1");
                                                }},
                                                new HashMap<String, Object>() {{
                                                    put("uid", "0x2");
                                                }}
                                        ));
                                    }},
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key-2");
                                        put("~pred1", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("uid", "0x2");
                                                }}
                                        ));
                                    }},
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key-3");
                                        put("pred1", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("uid", "0x3");
                                                }}
                                        ));
                                        put("~pred1", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("uid", "0x1");
                                                }},
                                                new HashMap<String, Object>() {{
                                                    put("uid", "0x4");
                                                }}
                                        ));
                                    }},
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key-4");
                                        put("pred2", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("uid", "0x5");
                                                }}
                                        ));
                                    }}
                            ));
                        }}
                ),
                Arrays.asList(
                        new RelatedEntity("pred1", "urn:ns:type:dest-key-1"),
                        new RelatedEntity("pred1", "urn:ns:type:dest-key-2"),
                        new RelatedEntity("pred1", "urn:ns:type:dest-key-3"),
                        new RelatedEntity("pred2", "urn:ns:type:dest-key-4")
                ),
                RELATED_ENTITY_COMPARATOR
        );
    }
}
