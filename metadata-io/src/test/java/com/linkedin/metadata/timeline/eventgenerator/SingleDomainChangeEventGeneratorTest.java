package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.Assert.*;

import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.sql.Timestamp;
import org.testng.annotations.Test;

/**
 * Tests that {@link SingleDomainChangeEventGenerator} correctly implements {@code getSemanticDiff}
 * (the legacy API still called by the timeline pipeline). Before the fix, the generator only
 * implemented the new {@code getChangeEvents} API, so {@code getSemanticDiff} fell through to the
 * base class and threw {@link UnsupportedOperationException}.
 */
public class SingleDomainChangeEventGeneratorTest {

  private static final String ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)";
  private static final String DOMAIN_URN_1 = "urn:li:domain:engineering";
  private static final String DOMAIN_URN_2 = "urn:li:domain:marketing";

  private static EntityAspect makeDomainAspect(String entityUrn, String domainUrn, long version) {
    String json =
        domainUrn != null
            ? String.format("{\"domains\": [\"%s\"]}", domainUrn)
            : "{\"domains\": []}";
    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(entityUrn);
    aspect.setAspect("domains");
    aspect.setVersion(version);
    aspect.setMetadata(json);
    aspect.setCreatedOn(new Timestamp(1000L * (version + 1)));
    aspect.setCreatedBy("urn:li:corpuser:tester");
    return aspect;
  }

  @Test
  public void testGetSemanticDiffDoesNotThrow() {
    SingleDomainChangeEventGenerator generator = new SingleDomainChangeEventGenerator();

    EntityAspect previous = makeDomainAspect(ENTITY_URN, null, 0);
    EntityAspect current = makeDomainAspect(ENTITY_URN, DOMAIN_URN_1, 1);

    // This must NOT throw UnsupportedOperationException
    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOMAIN, null, false);

    assertNotNull(tx);
    assertNotNull(tx.getChangeEvents());
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.DOMAIN);
    assertEquals(event.getOperation(), ChangeOperation.ADD);
    assertEquals(event.getEntityUrn(), ENTITY_URN);
    assertNotNull(event.getDescription(), "Domain ADD events must have a description");
    assertTrue(event.getDescription().contains("engineering"));
    assertTrue(event.getDescription().contains(ENTITY_URN));
  }

  @Test
  public void testDomainChanged() {
    SingleDomainChangeEventGenerator generator = new SingleDomainChangeEventGenerator();

    EntityAspect previous = makeDomainAspect(ENTITY_URN, DOMAIN_URN_1, 0);
    EntityAspect current = makeDomainAspect(ENTITY_URN, DOMAIN_URN_2, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOMAIN, null, false);

    assertNotNull(tx);
    // Domain change emits a REMOVE + ADD pair
    assertEquals(tx.getChangeEvents().size(), 2);

    long addCount =
        tx.getChangeEvents().stream().filter(e -> e.getOperation() == ChangeOperation.ADD).count();
    long removeCount =
        tx.getChangeEvents().stream()
            .filter(e -> e.getOperation() == ChangeOperation.REMOVE)
            .count();
    assertEquals(addCount, 1);
    assertEquals(removeCount, 1);

    // Both events must have descriptions
    for (ChangeEvent event : tx.getChangeEvents()) {
      assertNotNull(event.getDescription(), "Domain change events must have descriptions");
    }
  }

  @Test
  public void testDomainRemoved() {
    SingleDomainChangeEventGenerator generator = new SingleDomainChangeEventGenerator();

    EntityAspect previous = makeDomainAspect(ENTITY_URN, DOMAIN_URN_1, 0);
    EntityAspect current = makeDomainAspect(ENTITY_URN, null, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOMAIN, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);
    assertNotNull(
        tx.getChangeEvents().get(0).getDescription(),
        "Domain REMOVE events must have a description");
    assertTrue(tx.getChangeEvents().get(0).getDescription().contains("engineering"));
    assertEquals(tx.getChangeEvents().get(0).getOperation(), ChangeOperation.REMOVE);
  }

  @Test
  public void testNoDomainChange() {
    SingleDomainChangeEventGenerator generator = new SingleDomainChangeEventGenerator();

    EntityAspect previous = makeDomainAspect(ENTITY_URN, DOMAIN_URN_1, 0);
    EntityAspect current = makeDomainAspect(ENTITY_URN, DOMAIN_URN_1, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOMAIN, null, false);

    assertNotNull(tx);
    assertTrue(tx.getChangeEvents().isEmpty());
    assertEquals(tx.getSemVerChange(), SemanticChangeType.NONE);
  }
}
