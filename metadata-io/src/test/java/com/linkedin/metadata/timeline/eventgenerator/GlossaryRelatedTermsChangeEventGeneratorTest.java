package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.Assert.*;

import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.sql.Timestamp;
import java.util.Map;
import org.testng.annotations.Test;

public class GlossaryRelatedTermsChangeEventGeneratorTest {

  private static final String ENTITY_URN = "urn:li:glossaryTerm:myTerm";
  private static final String TERM_A = "urn:li:glossaryTerm:termA";
  private static final String TERM_B = "urn:li:glossaryTerm:termB";

  private static EntityAspect makeRelatedTermsAspect(
      String entityUrn,
      String[] isRelated,
      String[] hasRelated,
      String[] values,
      String[] related,
      long version) {
    StringBuilder json = new StringBuilder("{");
    json.append("\"isRelatedTerms\":[");
    appendTermArray(json, isRelated);
    json.append("],\"hasRelatedTerms\":[");
    appendTermArray(json, hasRelated);
    json.append("],\"values\":[");
    appendTermArray(json, values);
    json.append("],\"relatedTerms\":[");
    appendTermArray(json, related);
    json.append("]}");

    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(entityUrn);
    aspect.setAspect("glossaryRelatedTerms");
    aspect.setVersion(version);
    aspect.setMetadata(json.toString());
    aspect.setCreatedOn(new Timestamp(1000L * (version + 1)));
    aspect.setCreatedBy("urn:li:corpuser:tester");
    return aspect;
  }

  private static void appendTermArray(StringBuilder sb, String[] terms) {
    if (terms != null) {
      for (int i = 0; i < terms.length; i++) {
        if (i > 0) sb.append(",");
        sb.append("\"").append(terms[i]).append("\"");
      }
    }
  }

  @Test
  public void testIsRelatedTermAdded() {
    GlossaryRelatedTermsChangeEventGenerator generator =
        new GlossaryRelatedTermsChangeEventGenerator();

    EntityAspect previous =
        makeRelatedTermsAspect(
            ENTITY_URN, new String[] {}, new String[] {}, new String[] {}, new String[] {}, 0);
    EntityAspect current =
        makeRelatedTermsAspect(
            ENTITY_URN,
            new String[] {TERM_A},
            new String[] {},
            new String[] {},
            new String[] {},
            1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.GLOSSARY_TERM, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.GLOSSARY_TERM);
    assertEquals(event.getOperation(), ChangeOperation.ADD);
    assertEquals(event.getEntityUrn(), ENTITY_URN);
    assertTrue(event.getDescription().contains("Is A"));
  }

  @Test
  public void testIsRelatedTermRemoved() {
    GlossaryRelatedTermsChangeEventGenerator generator =
        new GlossaryRelatedTermsChangeEventGenerator();

    EntityAspect previous =
        makeRelatedTermsAspect(
            ENTITY_URN,
            new String[] {TERM_A},
            new String[] {},
            new String[] {},
            new String[] {},
            0);
    EntityAspect current =
        makeRelatedTermsAspect(
            ENTITY_URN, new String[] {}, new String[] {}, new String[] {}, new String[] {}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.GLOSSARY_TERM, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.GLOSSARY_TERM);
    assertEquals(event.getOperation(), ChangeOperation.REMOVE);
    assertEquals(event.getEntityUrn(), ENTITY_URN);
  }

  @Test
  public void testHasRelatedTermAdded() {
    GlossaryRelatedTermsChangeEventGenerator generator =
        new GlossaryRelatedTermsChangeEventGenerator();

    EntityAspect previous =
        makeRelatedTermsAspect(
            ENTITY_URN, new String[] {}, new String[] {}, new String[] {}, new String[] {}, 0);
    EntityAspect current =
        makeRelatedTermsAspect(
            ENTITY_URN,
            new String[] {},
            new String[] {TERM_A},
            new String[] {},
            new String[] {},
            1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.GLOSSARY_TERM, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.GLOSSARY_TERM);
    assertEquals(event.getOperation(), ChangeOperation.ADD);
    assertTrue(event.getDescription().contains("Has A"));
  }

  @Test
  public void testValuesTermAdded() {
    GlossaryRelatedTermsChangeEventGenerator generator =
        new GlossaryRelatedTermsChangeEventGenerator();

    EntityAspect previous =
        makeRelatedTermsAspect(
            ENTITY_URN, new String[] {}, new String[] {}, new String[] {}, new String[] {}, 0);
    EntityAspect current =
        makeRelatedTermsAspect(
            ENTITY_URN,
            new String[] {},
            new String[] {},
            new String[] {TERM_A},
            new String[] {},
            1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.GLOSSARY_TERM, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.GLOSSARY_TERM);
    assertEquals(event.getOperation(), ChangeOperation.ADD);
    assertTrue(event.getDescription().contains("Has Value"));
  }

  @Test
  public void testRelatedTermAdded() {
    GlossaryRelatedTermsChangeEventGenerator generator =
        new GlossaryRelatedTermsChangeEventGenerator();

    EntityAspect previous =
        makeRelatedTermsAspect(
            ENTITY_URN, new String[] {}, new String[] {}, new String[] {}, new String[] {}, 0);
    EntityAspect current =
        makeRelatedTermsAspect(
            ENTITY_URN,
            new String[] {},
            new String[] {},
            new String[] {},
            new String[] {TERM_A},
            1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.GLOSSARY_TERM, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.GLOSSARY_TERM);
    assertEquals(event.getOperation(), ChangeOperation.ADD);
    assertTrue(event.getDescription().contains("Is Related To"));
  }

  @Test
  public void testTermSwapped() {
    GlossaryRelatedTermsChangeEventGenerator generator =
        new GlossaryRelatedTermsChangeEventGenerator();

    EntityAspect previous =
        makeRelatedTermsAspect(
            ENTITY_URN,
            new String[] {TERM_A},
            new String[] {},
            new String[] {},
            new String[] {},
            0);
    EntityAspect current =
        makeRelatedTermsAspect(
            ENTITY_URN,
            new String[] {TERM_B},
            new String[] {},
            new String[] {},
            new String[] {},
            1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.GLOSSARY_TERM, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 2);

    long addCount =
        tx.getChangeEvents().stream().filter(e -> e.getOperation() == ChangeOperation.ADD).count();
    long removeCount =
        tx.getChangeEvents().stream()
            .filter(e -> e.getOperation() == ChangeOperation.REMOVE)
            .count();
    assertEquals(addCount, 1);
    assertEquals(removeCount, 1);
  }

  @Test
  public void testNoChange() {
    GlossaryRelatedTermsChangeEventGenerator generator =
        new GlossaryRelatedTermsChangeEventGenerator();

    EntityAspect previous =
        makeRelatedTermsAspect(
            ENTITY_URN,
            new String[] {TERM_A},
            new String[] {},
            new String[] {},
            new String[] {},
            0);
    EntityAspect current =
        makeRelatedTermsAspect(
            ENTITY_URN,
            new String[] {TERM_A},
            new String[] {},
            new String[] {},
            new String[] {},
            1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.GLOSSARY_TERM, null, false);

    assertNotNull(tx);
    assertTrue(tx.getChangeEvents().isEmpty());
    assertEquals(tx.getSemVerChange(), SemanticChangeType.NONE);
  }

  @Test
  public void testMultipleRelationshipTypesChanged() {
    GlossaryRelatedTermsChangeEventGenerator generator =
        new GlossaryRelatedTermsChangeEventGenerator();

    EntityAspect previous =
        makeRelatedTermsAspect(
            ENTITY_URN, new String[] {}, new String[] {}, new String[] {}, new String[] {}, 0);
    EntityAspect current =
        makeRelatedTermsAspect(
            ENTITY_URN,
            new String[] {TERM_A},
            new String[] {TERM_B},
            new String[] {},
            new String[] {},
            1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.GLOSSARY_TERM, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 2);

    long addCount =
        tx.getChangeEvents().stream().filter(e -> e.getOperation() == ChangeOperation.ADD).count();
    assertEquals(addCount, 2);

    boolean hasIsA =
        tx.getChangeEvents().stream().anyMatch(e -> e.getDescription().contains("Is A"));
    boolean hasHasA =
        tx.getChangeEvents().stream().anyMatch(e -> e.getDescription().contains("Has A"));
    assertTrue(hasIsA);
    assertTrue(hasHasA);
  }

  @Test
  public void testNonGlossaryTermCategoryIgnored() {
    GlossaryRelatedTermsChangeEventGenerator generator =
        new GlossaryRelatedTermsChangeEventGenerator();

    EntityAspect previous =
        makeRelatedTermsAspect(
            ENTITY_URN, new String[] {}, new String[] {}, new String[] {}, new String[] {}, 0);
    EntityAspect current =
        makeRelatedTermsAspect(
            ENTITY_URN,
            new String[] {TERM_A},
            new String[] {},
            new String[] {},
            new String[] {},
            1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.TAG, null, false);

    assertNotNull(tx);
    assertTrue(tx.getChangeEvents().isEmpty());
  }

  @Test
  public void testParametersContainTermUrnAndRelType() {
    GlossaryRelatedTermsChangeEventGenerator generator =
        new GlossaryRelatedTermsChangeEventGenerator();

    EntityAspect previous =
        makeRelatedTermsAspect(
            ENTITY_URN, new String[] {}, new String[] {}, new String[] {}, new String[] {}, 0);
    EntityAspect current =
        makeRelatedTermsAspect(
            ENTITY_URN,
            new String[] {TERM_A},
            new String[] {},
            new String[] {},
            new String[] {},
            1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.GLOSSARY_TERM, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    Map<String, Object> params = event.getParameters();
    assertNotNull(params);
    assertTrue(params.containsKey("termUrn"));
    assertTrue(params.containsKey("relationshipType"));
    assertEquals(params.get("termUrn"), TERM_A);
    assertEquals(params.get("relationshipType"), "Is A");
  }
}
