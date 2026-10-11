package com.linkedin.datahub.graphql.types.timeline.mappers;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.datahub.graphql.generated.ChangeOperationType;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.net.URISyntaxException;
import org.testng.annotations.Test;

public class ChangeEventMapperTest {

  @Test
  public void testOwnershipMapsDirectly() {
    // Backend OWNERSHIP now matches GraphQL OWNERSHIP — no mapping table needed
    ChangeEvent backendEvent =
        ChangeEvent.builder()
            .entityUrn("urn:li:dataset:test")
            .category(ChangeCategory.OWNERSHIP)
            .operation(ChangeOperation.ADD)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Owner added")
            .build();

    com.linkedin.datahub.graphql.generated.ChangeEvent result = ChangeEventMapper.map(backendEvent);
    assertEquals(result.getCategory(), ChangeCategoryType.OWNERSHIP);
  }

  @Test
  public void testMapPopulatesEntityUrn() {
    ChangeEvent backendEvent =
        ChangeEvent.builder()
            .entityUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)")
            .category(ChangeCategory.TAG)
            .operation(ChangeOperation.ADD)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Tag added")
            .build();

    com.linkedin.datahub.graphql.generated.ChangeEvent result = ChangeEventMapper.map(backendEvent);

    assertEquals(result.getUrn(), "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)");
    assertEquals(result.getCategory(), ChangeCategoryType.TAG);
    assertEquals(result.getOperation(), ChangeOperationType.ADD);
  }

  @Test
  public void testMapWithNullEntityUrnFallsBackToEmpty() {
    ChangeEvent backendEvent =
        ChangeEvent.builder()
            .entityUrn(null)
            .category(ChangeCategory.TAG)
            .operation(ChangeOperation.ADD)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Tag added")
            .build();

    com.linkedin.datahub.graphql.generated.ChangeEvent result = ChangeEventMapper.map(backendEvent);

    assertEquals(result.getUrn(), "empty");
  }

  @Test
  public void testMapPreservesParameters() {
    ChangeEvent backendEvent =
        ChangeEvent.builder()
            .entityUrn("urn:li:dataset:test")
            .category(ChangeCategory.OWNERSHIP)
            .operation(ChangeOperation.ADD)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Owner added")
            .parameters(
                ImmutableMap.of(
                    "ownerUrn", "urn:li:corpuser:jane",
                    "ownerType", "TECHNICAL_OWNER"))
            .build();

    com.linkedin.datahub.graphql.generated.ChangeEvent result = ChangeEventMapper.map(backendEvent);

    assertEquals(result.getCategory(), ChangeCategoryType.OWNERSHIP);
    assertEquals(result.getParameters().size(), 2);
  }

  @Test
  public void testMapWithAuditStampPopulatesAuditStamp() throws URISyntaxException {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(1700000000000L);
    auditStamp.setActor(Urn.createFromString("urn:li:corpuser:admin"));

    ChangeEvent backendEvent =
        ChangeEvent.builder()
            .entityUrn("urn:li:dataset:test")
            .category(ChangeCategory.DOCUMENTATION)
            .operation(ChangeOperation.MODIFY)
            .semVerChange(SemanticChangeType.PATCH)
            .description("Doc updated")
            .auditStamp(auditStamp)
            .build();

    com.linkedin.datahub.graphql.generated.ChangeEvent result = ChangeEventMapper.map(backendEvent);

    assertNotNull(result.getAuditStamp(), "AuditStamp should be mapped when present");
    assertEquals(result.getAuditStamp().getTime(), Long.valueOf(1700000000000L));
  }

  @Test
  public void testMapWithNullOperationDoesNotThrow() {
    // Some change events from older generators may lack an operation
    ChangeEvent backendEvent =
        ChangeEvent.builder()
            .entityUrn("urn:li:dataset:test")
            .category(ChangeCategory.TAG)
            .operation(null)
            .semVerChange(SemanticChangeType.MINOR)
            .description("legacy event")
            .build();

    com.linkedin.datahub.graphql.generated.ChangeEvent result = ChangeEventMapper.map(backendEvent);

    assertNull(result.getOperation());
    assertEquals(result.getDescription(), "legacy event");
  }
}
