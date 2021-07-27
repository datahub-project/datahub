package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;

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

    @Test
    public void testAddEdge() throws Exception {
        // test both directions
        Edge edge1 = new Edge(
                Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
                "DownstreamOf");

        _service.addEdge(edge1);

        List<String> edgeTypes = new ArrayList<>();
        edgeTypes.add("DownstreamOf");
        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setDirection(RelationshipDirection.OUTGOING);
        relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

        List<String> relatedUrns = _service.findRelatedUrns(
                "",
                newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                "",
                EMPTY_FILTER,
                edgeTypes,
                relationshipFilter,
                0,
                10);

        assertEquals(relatedUrns.size(), 1);
    }

    @Test
    public void testAddEdgeReverse() throws Exception {
        Edge edge1 = new Edge(
                Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
                Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                "DownstreamOf");

        _service.addEdge(edge1);

        List<String> edgeTypes = new ArrayList<>();
        edgeTypes.add("DownstreamOf");
        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setDirection(RelationshipDirection.INCOMING);
        relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

        List<String> relatedUrns = _service.findRelatedUrns(
                "",
                newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                "",
                EMPTY_FILTER,
                edgeTypes,
                relationshipFilter,
                0,
                10);

        assertEquals(relatedUrns.size(), 1);
    }

    @Test
    public void testRemoveEdgesFromNodeDeprecated() throws Exception {
        // TODO: test with both relationship directions
        Edge edge1 = new Edge(
                Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
                "DownstreamOf");

        _service.addEdge(edge1);

        List<String> edgeTypes = new ArrayList<>();
        edgeTypes.add("DownstreamOf");
        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setDirection(RelationshipDirection.INCOMING);
        relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

        List<String> relatedUrns = _service.findRelatedUrns(
                "",
                newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                "",
                EMPTY_FILTER,
                edgeTypes,
                relationshipFilter,
                0,
                10);

        assertEquals(relatedUrns.size(), 1);

        _service.removeEdgesFromNode(Urn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                edgeTypes,
                relationshipFilter);

        List<String> relatedUrnsPostDelete = _service.findRelatedUrns(
                "",
                newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                "",
                EMPTY_FILTER,
                edgeTypes,
                relationshipFilter,
                0,
                10);

        assertEquals(relatedUrnsPostDelete.size(), 0);
    }

    @Nonnull
    @Override
    protected GraphService getGraphService() throws Exception {
        _service.clear();
        return _service;
    }

    @Override
    protected void syncAfterWrite() throws Exception { }

    @Test
    public void testClear() throws Exception {
        // TODO: test with both relationship directions
        Edge edge1 = new Edge(
                Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
                Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                "DownstreamOf");

        _service.addEdge(edge1);

        List<String> edgeTypes = new ArrayList<>();
        edgeTypes.add("DownstreamOf");
        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setDirection(RelationshipDirection.INCOMING);
        relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

        List<String> relatedUrns = _service.findRelatedUrns(
                "",
                newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                "",
                EMPTY_FILTER,
                edgeTypes,
                relationshipFilter,
                0,
                10);

        assertEquals(relatedUrns.size(), 1);

        _service.clear();

        List<String> relatedUrnsPostDelete = _service.findRelatedUrns(
                "",
                newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                "",
                EMPTY_FILTER,
                edgeTypes,
                relationshipFilter,
                0,
                10);

        assertEquals(relatedUrnsPostDelete.size(), 0);
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

        // source type
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        "sourceTypeFilter",
                        null,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList()),
                "@filter(\n" +
                        "    uid(sourceTypeFilter)\n" +
                        "  )"
        );

        // destination type not supported without restricting relationship types
        // there must be as many relation type filter names as there are relationships
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        "destinationTypeFilter",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Arrays.asList("RelationshipTypeFilter"),
                        Arrays.asList("relationship")),
                "@filter(\n" +
                        "    (\n" +
                        "      uid(RelationshipTypeFilter) AND uid_in(<relationship>, uid(destinationTypeFilter))\n" +
                        "    )\n" +
                        "  )"
        );

        // source filters
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        null,
                        Arrays.asList("sourceFilter"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList()),
                "@filter(\n" +
                        "    uid(sourceFilter)\n" +
                        "  )"
        );
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        null,
                        Arrays.asList("sourceFilter1", "sourceFilter2"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList()),
                "@filter(\n" +
                        "    uid(sourceFilter1) AND\n" +
                        "    uid(sourceFilter2)\n" +
                        "  )"
        );

        // destination filter not supported without restricting relationship types
        // there must be as many relation type filter names as there are relationships
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        null,
                        Collections.emptyList(),
                        Arrays.asList("destinationFilter"),
                        Arrays.asList("RelationshipTypeFilter"),
                        Arrays.asList("relationship")),
                "@filter(\n" +
                        "    (\n" +
                        "      uid(RelationshipTypeFilter) AND uid_in(<relationship>, uid(destinationFilter))\n" +
                        "    )\n" +
                        "  )"
        );
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        null,
                        Collections.emptyList(),
                        Arrays.asList("destinationFilter1", "destinationFilter2"),
                        Arrays.asList("RelationshipTypeFilter"),
                        Arrays.asList("relationship")),
                "@filter(\n" +
                        "    (\n" +
                        "      uid(RelationshipTypeFilter) AND uid_in(<relationship>, uid(destinationFilter1)) AND uid_in(<relationship>, uid(destinationFilter2))\n" +
                        "    )\n" +
                        "  )"
        );
        assertEquals(
                DgraphGraphService.getFilterConditions(
                        null,
                        null,
                        Collections.emptyList(),
                        Arrays.asList("destinationFilter1", "destinationFilter2"),
                        Arrays.asList("RelationshipTypeFilter1", "RelationshipTypeFilter2"),
                        Arrays.asList("relationship1", "relationship2")),
                "@filter(\n" +
                        "    (\n" +
                        "      uid(RelationshipTypeFilter1) AND uid_in(<relationship1>, uid(destinationFilter1)) AND uid_in(<relationship1>, uid(destinationFilter2)) OR\n" +
                        "      uid(RelationshipTypeFilter2) AND uid_in(<relationship2>, uid(destinationFilter1)) AND uid_in(<relationship2>, uid(destinationFilter2))\n" +
                        "    )\n" +
                        "  )"
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
                "@filter(\n" +
                        "    (\n" +
                        "      uid(relationshipTypeFilter1) OR\n" +
                        "      uid(relationshipTypeFilter2)\n" +
                        "    )\n" +
                        "  )"
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
                "@filter(\n" +
                        "    uid(sourceTypeFilter) AND\n" +
                        "    uid(sourceFilter1) AND\n" +
                        "    uid(sourceFilter2) AND\n" +
                        "    (\n" +
                        "      uid(relationshipTypeFilter1) AND uid_in(<relationship1>, uid(destinationTypeFilter)) AND uid_in(<relationship1>, uid(destinationFilter1)) AND uid_in(<relationship1>, uid(destinationFilter2)) OR\n" +
                        "      uid(relationshipTypeFilter2) AND uid_in(<relationship2>, uid(destinationTypeFilter)) AND uid_in(<relationship2>, uid(destinationFilter1)) AND uid_in(<relationship2>, uid(destinationFilter2))\n" +
                        "    )\n" +
                        "  )"
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
                "uid(relationshipFilter) AND uid_in(<relationship>, uid(destinationTypeFilter)) AND uid_in(<relationship>, uid(destinationFilter))"
        );

        assertEquals(
                DgraphGraphService.getRelationshipCondition(
                        "relationship",
                        "relationshipFilter",
                        "destinationTypeFilter",
                        Arrays.asList("destinationFilter1", "destinationFilter2")),
                "uid(relationshipFilter) AND uid_in(<relationship>, uid(destinationTypeFilter)) AND uid_in(<relationship>, uid(destinationFilter1)) AND uid_in(<relationship>, uid(destinationFilter2))"
        );

        assertEquals(
                DgraphGraphService.getRelationshipCondition(
                        "relationship",
                        "relationshipFilter",
                        null,
                        Arrays.asList("destinationFilter1", "destinationFilter2")),
                "uid(relationshipFilter) AND uid_in(<relationship>, uid(destinationFilter1)) AND uid_in(<relationship>, uid(destinationFilter2))"
        );
    }

    @Test
    public void testGetQueryForRelatedUrns() {
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
                        new RelationshipFilter().setCriteria(EMPTY_FILTER.getCriteria()).setDirection(RelationshipDirection.INCOMING),
                        0, 100
                ),
                "query {\n" +
                        "  sourceType as var(func: eq(<type>, \"sourceType\"))\n" +
                        "  destinationTypeType as var(func: eq(<type>, \"destinationType\"))\n" +
                        "  sourceFilter1 as var(func: eq(<urn>, \"urn:ns:type:source-key\"))\n" +
                        "  sourceFilter2 as var(func: eq(<key>, \"source-key\"))\n" +
                        "  destinationFilter1 as var(func: eq(<urn>, \"urn:ns:type:dest-key\"))\n" +
                        "  destinationFilter2 as var(func: eq(<key>, \"dest-key\"))\n" +
                        "  relationshipType1 as var(func: has(<relationship1>))\n" +
                        "  relationshipType2 as var(func: has(<relationship2>))\n" +
                        "\n" +
                        "  result (func: uid(sourceFilter1), first: 100, offset: 0) @filter(\n" +
                        "    uid(sourceType) AND\n" +
                        "    uid(sourceFilter1) AND\n" +
                        "    uid(sourceFilter2) AND\n" +
                        "    (\n" +
                        "      uid(relationshipType1) AND uid_in(<relationship1>, uid(destinationTypeType)) AND uid_in(<relationship1>, uid(destinationFilter1)) AND uid_in(<relationship1>, uid(destinationFilter2)) OR\n" +
                        "      uid(relationshipType2) AND uid_in(<relationship2>, uid(destinationTypeType)) AND uid_in(<relationship2>, uid(destinationFilter1)) AND uid_in(<relationship2>, uid(destinationFilter2))\n" +
                        "    )\n" +
                        "  ) {\n" +
                        "    uid\n" +
                        "    <urn>\n" +
                        "    <type>\n" +
                        "    <key>\n" +
                        "    <relationship1> { uid <urn> <type> <key> }\n" +
                        "    <relationship2> { uid <urn> <type> <key> }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testGetDestinationUrnsFromResponseData() {
        // no results
        assertEquals(
                DgraphGraphService.getDestinationUrnsFromResponseData(
                        new HashMap<String, Object>() {{
                            put("result", Collections.emptyList());
                        }},
                        Collections.emptyList()
                ),
                Collections.emptyList()
        );

        // one result and one relationship
        assertEquals(
                DgraphGraphService.getDestinationUrnsFromResponseData(
                        new HashMap<String, Object>() {{
                            put("result", Arrays.asList(
                                    new HashMap<String, Object>() {{
                                        put("relationship", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("urn", "urn:ns:type:dest-key");
                                                }}
                                        ));
                                    }}
                            ));
                        }},
                        Arrays.asList("relationship")
                ),
                Arrays.asList("urn:ns:type:dest-key")
        );

        // multiple results and one relationship
        assertEquals(
                DgraphGraphService.getDestinationUrnsFromResponseData(
                        new HashMap<String, Object>() {{
                            put("result", Arrays.asList(
                                    new HashMap<String, Object>() {{
                                        put("relationship", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("urn", "urn:ns:type:dest-key-1");
                                                }}
                                        ));
                                    }},
                                    new HashMap<String, Object>() {{
                                        put("relationship", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("urn", "urn:ns:type:dest-key-2");
                                                }}
                                        ));
                                    }}
                            ));
                        }},
                        Arrays.asList("relationship")
                ),
                Arrays.asList("urn:ns:type:dest-key-1", "urn:ns:type:dest-key-2")
        );

        // multiple results and relationships
        assertEquals(
                DgraphGraphService.getDestinationUrnsFromResponseData(
                        new HashMap<String, Object>() {{
                            put("result", Arrays.asList(
                                    new HashMap<String, Object>() {{
                                        put("relationship1", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("urn", "urn:ns:type:dest-key-1");
                                                }}
                                        ));
                                        put("relationship2", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("urn", "urn:ns:type:dest-key-2");
                                                }}
                                        ));
                                    }},
                                    new HashMap<String, Object>() {{
                                        put("relationship2", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("urn", "urn:ns:type:dest-key-3");
                                                }}
                                        ));
                                        put("relationship1", Arrays.asList(
                                                new HashMap<String, Object>() {{
                                                    put("urn", "urn:ns:type:dest-key-4");
                                                }}
                                        ));
                                    }}
                            ));
                        }},
                        Arrays.asList("relationship1", "relationship2")
                ),
                Arrays.asList("urn:ns:type:dest-key-1", "urn:ns:type:dest-key-2", "urn:ns:type:dest-key-4", "urn:ns:type:dest-key-3")
        );
    }
}
