package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityExistsBatchLoaderTest {

  private static final Urn A = UrnUtils.getUrn("urn:li:corpuser:a");
  private static final Urn B = UrnUtils.getUrn("urn:li:corpuser:b");
  private static final Urn C = UrnUtils.getUrn("urn:li:corpuser:c");

  private EntityService<?> _entityService;
  private QueryContext _context;

  @BeforeMethod
  public void setup() {
    _entityService = Mockito.mock(EntityService.class);
    _context = getMockAllowContext();
  }

  @SuppressWarnings("unchecked")
  private void stubExists(Set<Urn> existing) {
    Mockito.when(_entityService.exists(any(OperationContext.class), any(Collection.class)))
        .thenReturn(existing);
  }

  @SuppressWarnings("unchecked")
  private ArgumentCaptor<Collection<Urn>> captureExists(int times) {
    final ArgumentCaptor<Collection<Urn>> captor = ArgumentCaptor.forClass(Collection.class);
    Mockito.verify(_entityService, Mockito.times(times))
        .exists(any(OperationContext.class), captor.capture());
    return captor;
  }

  /** N keys should cost one read, not N. */
  @Test
  public void testCollapsesToASingleCall() {
    stubExists(Set.of(A, C));

    final List<Boolean> results =
        EntityExistsBatchLoader.batchLoad(List.of(A, B, C), _context, _entityService);

    assertEquals(results, List.of(true, false, true));
    assertEquals(captureExists(1).getValue().size(), 3);
  }

  /** Duplicate keys are deduplicated for the read, but each still gets its own answer. */
  @Test
  public void testDuplicateKeysMapBackPositionally() {
    stubExists(Set.of(A));

    final List<Boolean> results =
        EntityExistsBatchLoader.batchLoad(List.of(A, B, A, B, A), _context, _entityService);

    assertEquals(results, List.of(true, false, true, false, true));
    assertEquals(captureExists(1).getValue().size(), 2);
  }

  @Test
  public void testNoneExisting() {
    stubExists(Set.of());

    final List<Boolean> results =
        EntityExistsBatchLoader.batchLoad(List.of(A, B), _context, _entityService);

    assertEquals(results, List.of(false, false));
  }

  /** A read failure must throw, not report the entity as absent. */
  @Test
  public void testFailurePropagatesRatherThanReportingAbsent() {
    Mockito.when(_entityService.exists(any(OperationContext.class), any(Collection.class)))
        .thenThrow(new RuntimeException("primary store unavailable"));

    assertThrows(
        RuntimeException.class,
        () -> EntityExistsBatchLoader.batchLoad(List.of(A, B), _context, _entityService));
  }
}
