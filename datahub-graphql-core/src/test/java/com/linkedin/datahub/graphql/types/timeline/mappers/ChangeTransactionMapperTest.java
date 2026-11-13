package com.linkedin.datahub.graphql.types.timeline.mappers;

import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.generated.ChangeOperationType;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

public class ChangeTransactionMapperTest {

  @Test
  public void testNullSemVerDefaultsToNone() {
    ChangeTransaction input =
        ChangeTransaction.builder()
            .timestamp(1000L)
            .semVer(null)
            .semVerChange(SemanticChangeType.MINOR)
            .changeEvents(Collections.emptyList())
            .build();

    com.linkedin.datahub.graphql.generated.ChangeTransaction result =
        ChangeTransactionMapper.map(input);

    assertEquals(result.getLastSemanticVersion(), "none");
  }

  @Test
  public void testNonNullSemVerPassesThrough() {
    ChangeTransaction input =
        ChangeTransaction.builder()
            .timestamp(2000L)
            .semVer("0.1.0-computed")
            .semVerChange(SemanticChangeType.MINOR)
            .changeEvents(Collections.emptyList())
            .build();

    com.linkedin.datahub.graphql.generated.ChangeTransaction result =
        ChangeTransactionMapper.map(input);

    assertEquals(result.getLastSemanticVersion(), "0.1.0-computed");
  }

  @Test
  public void testTimestampAndActorMapped() {
    ChangeTransaction input =
        ChangeTransaction.builder()
            .timestamp(123456789L)
            .actor("urn:li:corpuser:jane")
            .semVerChange(SemanticChangeType.NONE)
            .changeEvents(Collections.emptyList())
            .build();

    com.linkedin.datahub.graphql.generated.ChangeTransaction result =
        ChangeTransactionMapper.map(input);

    assertEquals(result.getTimestampMillis(), 123456789L);
    assertEquals(result.getActor(), "urn:li:corpuser:jane");
  }

  @Test
  public void testChangeTypeDefaultsToModify() {
    ChangeTransaction input =
        ChangeTransaction.builder()
            .timestamp(1000L)
            .semVerChange(SemanticChangeType.NONE)
            .changeEvents(Collections.emptyList())
            .build();

    com.linkedin.datahub.graphql.generated.ChangeTransaction result =
        ChangeTransactionMapper.map(input);

    assertEquals(result.getChangeType(), ChangeOperationType.MODIFY);
    assertEquals(result.getVersionStamp(), "none");
  }

  @Test
  public void testEventsWithNullCategoryAreFilteredOut() {
    // Error-wrapped events from computeDiff's exception handler have null category/operation.
    // The mapper should filter these out rather than passing them through.
    ChangeEvent validEvent =
        ChangeEvent.builder()
            .entityUrn("urn:li:dataset:test")
            .category(ChangeCategory.TAG)
            .operation(ChangeOperation.ADD)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Tag added")
            .build();
    ChangeEvent errorEvent =
        ChangeEvent.builder()
            .entityUrn(null)
            .category(null)
            .operation(null)
            .semVerChange(SemanticChangeType.EXCEPTIONAL)
            .description("UnsupportedOperationException:some error")
            .build();

    ChangeTransaction input =
        ChangeTransaction.builder()
            .timestamp(1000L)
            .semVerChange(SemanticChangeType.MINOR)
            .changeEvents(Arrays.asList(validEvent, errorEvent))
            .build();

    com.linkedin.datahub.graphql.generated.ChangeTransaction result =
        ChangeTransactionMapper.map(input);

    assertEquals(
        result.getChanges().size(), 1, "Error event with null category should be filtered");
    assertEquals(result.getChanges().get(0).getDescription(), "Tag added");
  }

  @Test
  public void testEventsWithNullOperationAreFilteredOut() {
    ChangeEvent eventNullOp =
        ChangeEvent.builder()
            .entityUrn("urn:li:dataset:test")
            .category(ChangeCategory.DOMAIN)
            .operation(null)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Has category but no operation")
            .build();

    ChangeTransaction input =
        ChangeTransaction.builder()
            .timestamp(1000L)
            .semVerChange(SemanticChangeType.MINOR)
            .changeEvents(Collections.singletonList(eventNullOp))
            .build();

    com.linkedin.datahub.graphql.generated.ChangeTransaction result =
        ChangeTransactionMapper.map(input);

    assertTrue(result.getChanges().isEmpty(), "Event with null operation should be filtered");
  }

  @Test
  public void testAllValidEventsAreMapped() {
    ChangeEvent event1 =
        ChangeEvent.builder()
            .entityUrn("urn:li:dataset:test")
            .category(ChangeCategory.TAG)
            .operation(ChangeOperation.ADD)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Tag added")
            .build();
    ChangeEvent event2 =
        ChangeEvent.builder()
            .entityUrn("urn:li:dataset:test")
            .category(ChangeCategory.OWNERSHIP)
            .operation(ChangeOperation.REMOVE)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Owner removed")
            .build();

    ChangeTransaction input =
        ChangeTransaction.builder()
            .timestamp(5000L)
            .semVer("0.2.0-computed")
            .semVerChange(SemanticChangeType.MINOR)
            .changeEvents(Arrays.asList(event1, event2))
            .build();

    com.linkedin.datahub.graphql.generated.ChangeTransaction result =
        ChangeTransactionMapper.map(input);

    assertEquals(result.getChanges().size(), 2);
  }

  @Test
  public void testEmptyEventsListProducesEmptyChanges() {
    ChangeTransaction input =
        ChangeTransaction.builder()
            .timestamp(1000L)
            .semVerChange(SemanticChangeType.NONE)
            .changeEvents(Collections.emptyList())
            .build();

    com.linkedin.datahub.graphql.generated.ChangeTransaction result =
        ChangeTransactionMapper.map(input);

    assertNotNull(result.getChanges());
    assertTrue(result.getChanges().isEmpty());
  }
}
