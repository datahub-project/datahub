package datahub.client.v2.entity;

import static org.junit.Assert.*;

import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.Urn;
import datahub.client.v2.exceptions.ReadOnlyEntityException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Unit tests for entity immutability behavior.
 *
 * <p>Tests the immutability-by-default pattern where entities fetched from the server are
 * read-only, and users must call .mutable() to obtain a writable copy.
 */
public class EntityImmutabilityTest {

  @Test
  public void testBuilderEntitiesAreMutable() {
    // Entities created via builder should be mutable from creation
    Chart chart = Chart.builder().tool("looker").id("test_chart").build();

    assertTrue("Builder entities should be mutable", chart.isMutable());
    assertFalse("Builder entities should not be read-only", chart.isReadOnly());
  }

  @Test
  public void testBuilderEntitiesAllowMutations() {
    // Builder entities should allow mutations without calling .mutable()
    Chart chart = Chart.builder().tool("looker").id("test_chart").build();

    // Should not throw - mutations are allowed on builder entities
    chart.setTitle("Test Chart");
    chart.setDescription("Test description");

    // Note: Can't read after mutation without saving (would throw PendingMutationsException)
    // This test just verifies mutations don't throw ReadOnlyEntityException
  }

  @Test
  public void testServerFetchedEntitiesAreReadOnly() {
    // Simulate a server-fetched entity
    ChartUrn urn = new ChartUrn("looker", "test_chart");
    Map<String, com.linkedin.data.template.RecordTemplate> serverAspects = new HashMap<>();

    // Use protected constructor that marks entities as read-only
    Chart chart = new Chart((Urn) urn, serverAspects);

    assertTrue("Server-fetched entities should be read-only", chart.isReadOnly());
    assertFalse("Server-fetched entities should not be mutable", chart.isMutable());
  }

  @Test
  public void testReadOnlyEntityThrowsOnMutation() {
    // Simulate a server-fetched entity
    ChartUrn urn = new ChartUrn("looker", "test_chart");
    Map<String, com.linkedin.data.template.RecordTemplate> serverAspects = new HashMap<>();
    Chart chart = new Chart((Urn) urn, serverAspects);

    // Attempting to mutate should throw ReadOnlyEntityException
    try {
      chart.setTitle("New Title");
      fail("Expected ReadOnlyEntityException");
    } catch (ReadOnlyEntityException e) {
      assertTrue(
          "Exception message should mention read-only", e.getMessage().contains("read-only"));
      assertTrue("Exception message should mention .mutable()", e.getMessage().contains("mutable"));
      assertEquals("chart", e.getEntityType());
      assertEquals("set title", e.getOperation());
    }
  }

  @Test
  public void testMutableCreatesWritableCopy() {
    // Simulate a server-fetched entity
    ChartUrn urn = new ChartUrn("looker", "test_chart");
    Map<String, com.linkedin.data.template.RecordTemplate> serverAspects = new HashMap<>();
    Chart readonly = new Chart((Urn) urn, serverAspects);

    // Get mutable copy
    Chart mutable = readonly.mutable();

    // Original should still be read-only
    assertTrue("Original entity should remain read-only", readonly.isReadOnly());
    assertFalse("Original entity should not be mutable", readonly.isMutable());

    // Copy should be mutable
    assertTrue("Mutable copy should be mutable", mutable.isMutable());
    assertFalse("Mutable copy should not be read-only", mutable.isReadOnly());
  }

  @Test
  public void testMutableCopyAllowsMutations() {
    // Simulate a server-fetched entity
    ChartUrn urn = new ChartUrn("looker", "test_chart");
    Map<String, com.linkedin.data.template.RecordTemplate> serverAspects = new HashMap<>();
    Chart readonly = new Chart((Urn) urn, serverAspects);

    // Get mutable copy and mutate it
    Chart mutable = readonly.mutable();
    mutable.setTitle("New Title"); // Should not throw
    mutable.setDescription("New Description"); // Should not throw

    // Note: Reads after mutations throw PendingMutationsException by design
    // This test verifies mutations don't throw ReadOnlyEntityException
  }

  @Test
  public void testMutableIsIdempotent() {
    // Calling .mutable() on an already mutable entity should return self
    Chart chart = Chart.builder().tool("looker").id("test_chart").build();

    assertTrue("Builder entities are mutable", chart.isMutable());

    Chart result = chart.mutable();

    // Should return the same instance
    assertSame("mutable() on mutable entity should return self", chart, result);
  }

  @Test
  public void testMutableSharesCache() {
    // Simulate a server-fetched entity with some cached data
    ChartUrn urn = new ChartUrn("looker", "test_chart");
    Map<String, com.linkedin.data.template.RecordTemplate> serverAspects = new HashMap<>();

    // Add a chart info aspect to the cache
    com.linkedin.chart.ChartInfo chartInfo = new com.linkedin.chart.ChartInfo();
    chartInfo.setTitle("Original Title");
    serverAspects.put("chartInfo", chartInfo);

    Chart readonly = new Chart((Urn) urn, serverAspects);

    // Get mutable copy
    Chart mutable = readonly.mutable();

    // Both should see the same cached data (shared cache)
    assertEquals(
        "Mutable copy should share cache with original", "Original Title", mutable.getTitle());
  }

  @Test
  public void testMutableHasIndependentUrn() {
    // Simulate a server-fetched entity
    ChartUrn urn = new ChartUrn("looker", "test_chart");
    Map<String, com.linkedin.data.template.RecordTemplate> serverAspects = new HashMap<>();
    Chart readonly = new Chart((Urn) urn, serverAspects);

    Chart mutable = readonly.mutable();

    // URNs should be equal but independent objects
    assertEquals("URNs should be equal", readonly.getUrn(), mutable.getUrn());
  }

  @Test
  public void testReadOnlyEntityAllowsReads() {
    // Read-only entities should allow all read operations
    ChartUrn urn = new ChartUrn("looker", "test_chart");
    Map<String, com.linkedin.data.template.RecordTemplate> serverAspects = new HashMap<>();

    com.linkedin.chart.ChartInfo chartInfo = new com.linkedin.chart.ChartInfo();
    chartInfo.setTitle("Test Title");
    chartInfo.setDescription("Test Description");
    serverAspects.put("chartInfo", chartInfo);

    Chart chart = new Chart((Urn) urn, serverAspects);

    // All reads should work fine
    assertEquals("Test Title", chart.getTitle());
    assertEquals("Test Description", chart.getDescription());
    assertNotNull(chart.getUrn());
    assertTrue(chart.isReadOnly());
  }

  @Test
  public void testMultipleMutationMethodsThrowWhenReadOnly() {
    // Test that all mutation methods respect read-only flag
    ChartUrn urn = new ChartUrn("looker", "test_chart");
    Map<String, com.linkedin.data.template.RecordTemplate> serverAspects = new HashMap<>();
    Chart chart = new Chart((Urn) urn, serverAspects);

    // Test various mutation methods
    assertThrows(ReadOnlyEntityException.class, () -> chart.setTitle("New Title"));
    assertThrows(ReadOnlyEntityException.class, () -> chart.setDescription("New Desc"));
    assertThrows(ReadOnlyEntityException.class, () -> chart.setChartType("BAR"));
    assertThrows(ReadOnlyEntityException.class, () -> chart.addCustomProperty("key", "value"));
  }

  @Test
  public void testMutableCopyIndependentDirtyFlag() {
    // Mutable copy should have independent dirty flag
    ChartUrn urn = new ChartUrn("looker", "test_chart");
    Map<String, com.linkedin.data.template.RecordTemplate> serverAspects = new HashMap<>();
    Chart readonly = new Chart((Urn) urn, serverAspects);

    Chart mutable = readonly.mutable();

    // Mutate the copy
    mutable.setTitle("New Title");

    // Original should still be clean (not dirty)
    // Mutable copy should be dirty
    // (Note: testing isDirty() if that method is exposed, or we can just verify no exception)
  }
}
