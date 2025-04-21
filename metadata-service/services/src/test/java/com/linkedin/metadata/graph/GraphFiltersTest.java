package com.linkedin.metadata.graph;

import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static org.testng.Assert.*;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class GraphFiltersTest {

  @Test
  public void testStaticFactoryMethods() {
    // Test incomingFilter
    Filter sourceFilter = new Filter();
    GraphFilters incomingFilters = GraphFilters.incomingFilter(sourceFilter);

    assertEquals(incomingFilters.getSourceEntityFilter(), sourceFilter);
    assertEquals(incomingFilters.getDestinationEntityFilter(), EMPTY_FILTER);
    assertEquals(
        incomingFilters.getRelationshipFilter().getDirection(), RelationshipDirection.INCOMING);
    assertNull(incomingFilters.getSourceTypes());
    assertNull(incomingFilters.getDestinationTypes());
    assertTrue(incomingFilters.getRelationshipTypes().isEmpty());

    // Test outgoingFilter
    GraphFilters outgoingFilters = GraphFilters.outgoingFilter(sourceFilter);

    assertEquals(outgoingFilters.getSourceEntityFilter(), sourceFilter);
    assertEquals(outgoingFilters.getDestinationEntityFilter(), EMPTY_FILTER);
    assertEquals(
        outgoingFilters.getRelationshipFilter().getDirection(), RelationshipDirection.OUTGOING);
    assertNull(outgoingFilters.getSourceTypes());
    assertNull(outgoingFilters.getDestinationTypes());
    assertTrue(outgoingFilters.getRelationshipTypes().isEmpty());

    // Test from method
    Set<String> relationshipTypes = new HashSet<>(Arrays.asList("HAS", "OWNS"));
    RelationshipFilter relationshipFilter =
        new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING);

    GraphFilters fromFilters =
        GraphFilters.from(sourceFilter, relationshipTypes, relationshipFilter);

    assertEquals(fromFilters.getSourceEntityFilter(), sourceFilter);
    assertEquals(fromFilters.getDestinationEntityFilter(), EMPTY_FILTER);
    assertEquals(fromFilters.getRelationshipFilter(), relationshipFilter);
    assertNull(fromFilters.getSourceTypes());
    assertNull(fromFilters.getDestinationTypes());
    assertEquals(fromFilters.getRelationshipTypes(), new HashSet<>(relationshipTypes));

    // Test ALL static instance
    assertEquals(GraphFilters.ALL.getSourceEntityFilter(), EMPTY_FILTER);
    assertEquals(GraphFilters.ALL.getDestinationEntityFilter(), EMPTY_FILTER);
    assertEquals(
        GraphFilters.ALL.getRelationshipFilter().getDirection(), RelationshipDirection.INCOMING);
    assertNull(GraphFilters.ALL.getSourceTypes());
    assertNull(GraphFilters.ALL.getDestinationTypes());
    assertTrue(GraphFilters.ALL.getRelationshipTypes().isEmpty());
  }

  @Test
  public void testConstructor() {
    Filter sourceFilter = new Filter();
    Filter destFilter = new Filter();
    Set<String> sourceTypes = new HashSet<>(Arrays.asList("dataset", "schemaField"));
    Set<String> destTypes = new HashSet<>(Arrays.asList("dataset", "schemaField"));
    Set<String> relationshipTypes = new HashSet<>(Arrays.asList("DownstreamOf", "Consumes"));
    RelationshipFilter relationshipFilter =
        new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING);

    GraphFilters filters =
        new GraphFilters(
            sourceFilter,
            destFilter,
            sourceTypes,
            destTypes,
            relationshipTypes,
            relationshipFilter);

    assertEquals(filters.getSourceEntityFilter(), sourceFilter);
    assertEquals(filters.getDestinationEntityFilter(), destFilter);
    assertEquals(filters.getSourceTypes(), sourceTypes);
    assertEquals(filters.getDestinationTypes(), destTypes);
    assertEquals(filters.getRelationshipTypes(), relationshipTypes);
    assertEquals(filters.getRelationshipFilter(), relationshipFilter);
    assertEquals(filters.getRelationshipDirection(), RelationshipDirection.OUTGOING);
  }

  @Test
  public void testConstructorWithNullRelationshipTypes() {
    Filter sourceFilter = new Filter();
    Filter destFilter = new Filter();
    RelationshipFilter relationshipFilter =
        new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING);

    GraphFilters filters =
        new GraphFilters(sourceFilter, destFilter, null, null, null, relationshipFilter);

    assertNotNull(filters.getRelationshipTypes());
    assertTrue(filters.getRelationshipTypes().isEmpty());
  }

  @Test
  public void testTypeFilterEnabledMethods() {
    GraphFilters emptyFilters =
        new GraphFilters(
            EMPTY_FILTER, EMPTY_FILTER, null, null, null, GraphFilters.INCOMING_FILTER);

    assertFalse(emptyFilters.isSourceTypesFilterEnabled());
    assertFalse(emptyFilters.isDestinationTypesFilterEnabled());

    GraphFilters emptySetFilters =
        new GraphFilters(
            EMPTY_FILTER,
            EMPTY_FILTER,
            new HashSet<>(),
            new HashSet<>(),
            null,
            GraphFilters.INCOMING_FILTER);

    assertFalse(emptySetFilters.isSourceTypesFilterEnabled());
    assertFalse(emptySetFilters.isDestinationTypesFilterEnabled());

    GraphFilters populatedFilters =
        new GraphFilters(
            EMPTY_FILTER,
            EMPTY_FILTER,
            new HashSet<>(Arrays.asList("Dataset")),
            new HashSet<>(Arrays.asList("User")),
            null,
            GraphFilters.INCOMING_FILTER);

    assertTrue(populatedFilters.isSourceTypesFilterEnabled());
    assertTrue(populatedFilters.isDestinationTypesFilterEnabled());
  }

  @Test
  public void testGetTypesOrderedMethods() {
    // Test with null types
    GraphFilters nullTypesFilters =
        new GraphFilters(
            EMPTY_FILTER, EMPTY_FILTER, null, null, null, GraphFilters.INCOMING_FILTER);

    assertNull(nullTypesFilters.getSourceTypesOrdered());
    assertNull(nullTypesFilters.getDestinationTypesOrdered());

    // Test with populated types (check ordering)
    Set<String> sourceTypes =
        new HashSet<>(Arrays.asList("Dashboard", "Chart", "Dataset", "Algorithm"));
    Set<String> destTypes = new HashSet<>(Arrays.asList("User", "Group", "Application"));
    Set<String> relationshipTypes = new HashSet<>(Arrays.asList("OWNS", "CREATES", "ACCESSES"));

    GraphFilters orderedFilters =
        new GraphFilters(
            EMPTY_FILTER,
            EMPTY_FILTER,
            sourceTypes,
            destTypes,
            relationshipTypes,
            GraphFilters.INCOMING_FILTER);

    List<String> orderedSourceTypes = orderedFilters.getSourceTypesOrdered();
    List<String> orderedDestTypes = orderedFilters.getDestinationTypesOrdered();
    List<String> orderedRelationshipTypes = orderedFilters.getRelationshipTypesOrdered();

    assertEquals(orderedSourceTypes, Arrays.asList("Algorithm", "Chart", "Dashboard", "Dataset"));
    assertEquals(orderedDestTypes, Arrays.asList("Application", "Group", "User"));
    assertEquals(orderedRelationshipTypes, Arrays.asList("ACCESSES", "CREATES", "OWNS"));
  }

  @Test
  public void testNoResultsByType() {
    // Test with null types
    GraphFilters nullTypesFilters =
        new GraphFilters(
            EMPTY_FILTER, EMPTY_FILTER, null, null, null, GraphFilters.INCOMING_FILTER);

    assertFalse(nullTypesFilters.noResultsByType());

    // Test with empty source types
    GraphFilters emptySourceFilters =
        new GraphFilters(
            EMPTY_FILTER, EMPTY_FILTER, new HashSet<>(), null, null, GraphFilters.INCOMING_FILTER);

    assertTrue(emptySourceFilters.noResultsByType());

    // Test with empty destination types
    GraphFilters emptyDestFilters =
        new GraphFilters(
            EMPTY_FILTER, EMPTY_FILTER, null, new HashSet<>(), null, GraphFilters.INCOMING_FILTER);

    assertTrue(emptyDestFilters.noResultsByType());

    // Test with populated types
    GraphFilters populatedFilters =
        new GraphFilters(
            EMPTY_FILTER,
            EMPTY_FILTER,
            new HashSet<>(Arrays.asList("Dataset")),
            new HashSet<>(Arrays.asList("User")),
            null,
            GraphFilters.INCOMING_FILTER);

    assertFalse(populatedFilters.noResultsByType());
  }
}
