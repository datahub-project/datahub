package com.linkedin.metadata.graph;

import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static com.linkedin.metadata.dao.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;
import static org.testng.Assert.assertEquals;


public class DgraphGraphServiceTest extends GraphServiceTestBase {

    private DgraphGraphService _service;

    @BeforeMethod
    public void init() {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 9080)
                .usePlaintext()
                .build();
        DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(channel);

        _service = new DgraphGraphService(new DgraphClient(stub));
    }

    @AfterMethod
    public void tearDown() { }

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

        schema.addField("newType", "newField");
        assertEquals(schema.getFields(), new HashSet<>(Arrays.asList("PredOne", "PredTwo", "newField")));
        assertEquals(schema.getTypes(), new HashMap<String, Set<String>>() {{
            put("ns:typeOne", new HashSet<>(Arrays.asList("PredOne", "PredTwo")));
            put("ns:typeTwo", new HashSet<>(Arrays.asList("PredTwo")));
            put("newType", new HashSet<>(Arrays.asList("newField")));
        }});

        schema.addField("ns:typeOne", "otherField");
        assertEquals(schema.getFields(), new HashSet<>(Arrays.asList("PredOne", "PredTwo", "newField", "otherField")));
        assertEquals(schema.getTypes(), new HashMap<String, Set<String>>() {{
            put("ns:typeOne", new HashSet<>(Arrays.asList("PredOne", "PredTwo", "otherField")));
            put("ns:typeTwo", new HashSet<>(Arrays.asList("PredTwo")));
            put("newType", new HashSet<>(Arrays.asList("newField")));
        }});

        schema.addField("ns:typeTwo", "PredTwo");
        assertEquals(schema.getFields(), new HashSet<>(Arrays.asList("PredOne", "PredTwo", "newField", "otherField")));
        assertEquals(schema.getTypes(), new HashMap<String, Set<String>>() {{
            put("ns:typeOne", new HashSet<>(Arrays.asList("PredOne", "PredTwo", "otherField")));
            put("ns:typeTwo", new HashSet<>(Arrays.asList("PredTwo")));
            put("newType", new HashSet<>(Arrays.asList("newField")));
        }});
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
    public void testGetQueryForRelatedUrnsOutgoing() {
        doTestGetQueryForRelatedUrnsDirection(RelationshipDirection.OUTGOING,
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
                        + "  }\n"
                        + "}"
        );
    }

    @Test
    public void testGetQueryForRelatedUrnsIncoming() {
        doTestGetQueryForRelatedUrnsDirection(RelationshipDirection.INCOMING,
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
                        + "  }\n"
                        + "}"
        );
    }

    @Test
    public void testGetQueryForRelatedUrnsUndirected() {
        doTestGetQueryForRelatedUrnsDirection(RelationshipDirection.UNDIRECTED,
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
                        + "  result (func: uid(destinationFilter1, destinationFilter2, destinationType, " +
                        "relationshipType1, relationshipType2, relationshipType3, relationshipType4), first: 100, offset: 0) @filter(\n"
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
                        + "  }\n"
                        + "}"
        );
    }

    private void doTestGetQueryForRelatedUrnsDirection(@Nonnull RelationshipDirection direction, @Nonnull String expectedQuery) {
        assertEquals(
                DgraphGraphService.getQueryForRelatedUrns(
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
                        new RelationshipFilter().setCriteria(EMPTY_FILTER.getCriteria()).setDirection(direction),
                        0, 100
                ),
                expectedQuery
        );
    }

    @Test
    public void testGetDestinationUrnsFromResponseData() {
        // no results
        assertEquals(
                DgraphGraphService.getDestinationUrnsFromResponseData(
                        new HashMap<String, Object>() {{
                            put("result", Collections.emptyList());
                        }}
                ),
                Collections.emptyList()
        );

        // one result and one relationship
        assertEquals(
                DgraphGraphService.getDestinationUrnsFromResponseData(
                        new HashMap<String, Object>() {{
                            put("result", Arrays.asList(
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key");
                                    }}
                            ));
                        }}
                ),
                Arrays.asList("urn:ns:type:dest-key")
        );

        // multiple results and one relationship
        assertEquals(
                DgraphGraphService.getDestinationUrnsFromResponseData(
                        new HashMap<String, Object>() {{
                            put("result", Arrays.asList(
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key-1");
                                    }},
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key-2");
                                    }}
                            ));
                        }}
                ),
                Arrays.asList("urn:ns:type:dest-key-1", "urn:ns:type:dest-key-2")
        );

        // multiple results and relationships
        assertEquals(
                DgraphGraphService.getDestinationUrnsFromResponseData(
                        new HashMap<String, Object>() {{
                            put("result", Arrays.asList(
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key-1");
                                    }},
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key-2");
                                    }},
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key-3");
                                    }},
                                    new HashMap<String, Object>() {{
                                        put("urn", "urn:ns:type:dest-key-4");
                                    }}
                            ));
                        }}
                ),
                Arrays.asList("urn:ns:type:dest-key-1", "urn:ns:type:dest-key-2", "urn:ns:type:dest-key-3", "urn:ns:type:dest-key-4")
        );
    }
}
