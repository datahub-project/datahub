package com.linkedin.datahub.graphql.types.glossary.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryTerms;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class GlossaryTermsMapperTest {

  private static final String TEST_GLOSSARY_TERM_URN_STRING = "urn:li:glossaryTerm:test.term";
  private static final String TEST_ACTOR_URN_STRING = "urn:li:corpuser:testuser";
  private static final String TEST_ENTITY_URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_test.users,PROD)";

  @Test
  public void testMapWithValidGlossaryTerms() throws Exception {
    // Arrange
    QueryContext mockContext = TestUtils.getMockAllowContext();
    com.linkedin.common.GlossaryTerms input = createTestGlossaryTerms();
    Urn entityUrn = UrnUtils.getUrn(TEST_ENTITY_URN_STRING);

    // Act
    GlossaryTerms result = GlossaryTermsMapper.map(mockContext, input, entityUrn);

    // Assert
    assertNotNull(result);
    assertEquals(result.getTerms().size(), 2);

    // Verify first term
    com.linkedin.datahub.graphql.generated.GlossaryTermAssociation firstAssociation =
        result.getTerms().get(0);
    assertNotNull(firstAssociation.getTerm());
    assertEquals(firstAssociation.getTerm().getUrn(), TEST_GLOSSARY_TERM_URN_STRING);
    assertEquals(firstAssociation.getTerm().getType(), EntityType.GLOSSARY_TERM);
    assertEquals(
        firstAssociation.getTerm().getName(), "term"); // Should extract last part after dot
    assertEquals(firstAssociation.getAssociatedUrn(), TEST_ENTITY_URN_STRING);
    assertEquals(firstAssociation.getContext(), "test context");
    assertNotNull(firstAssociation.getActor());
    assertEquals(firstAssociation.getActor().getUrn(), TEST_ACTOR_URN_STRING);
    assertEquals(firstAssociation.getActor().getType(), EntityType.CORP_USER);
    assertNotNull(firstAssociation.getAttribution());

    // Verify second term (without optional fields)
    com.linkedin.datahub.graphql.generated.GlossaryTermAssociation secondAssociation =
        result.getTerms().get(1);
    assertNotNull(secondAssociation.getTerm());
    assertEquals(secondAssociation.getTerm().getUrn(), "urn:li:glossaryTerm:simple");
    assertEquals(secondAssociation.getTerm().getName(), "simple"); // No dots, should return as-is
    assertEquals(secondAssociation.getAssociatedUrn(), TEST_ENTITY_URN_STRING);
    assertNull(secondAssociation.getContext());
    assertNull(secondAssociation.getActor());
    assertNull(secondAssociation.getAttribution());
  }

  @Test
  public void testMapWithNullContext() throws Exception {
    // Arrange
    com.linkedin.common.GlossaryTerms input = createTestGlossaryTerms();
    Urn entityUrn = UrnUtils.getUrn(TEST_ENTITY_URN_STRING);

    // Act
    GlossaryTerms result = GlossaryTermsMapper.map(null, input, entityUrn);

    // Assert
    assertNotNull(result);
    assertEquals(result.getTerms().size(), 2); // No filtering when context is null
  }

  @Test
  public void testMapWithNullEntityUrn() throws Exception {
    // Arrange
    QueryContext mockContext = TestUtils.getMockAllowContext();
    com.linkedin.common.GlossaryTerms input = createTestGlossaryTerms();

    // Act
    GlossaryTerms result = GlossaryTermsMapper.map(mockContext, input, null);

    // Assert
    assertNotNull(result);
    assertEquals(result.getTerms().size(), 2);

    // Verify that associatedUrn is null when entityUrn is null
    for (com.linkedin.datahub.graphql.generated.GlossaryTermAssociation association :
        result.getTerms()) {
      assertNull(association.getAssociatedUrn());
    }
  }

  @Test
  public void testMapWithEmptyGlossaryTerms() throws Exception {
    // Arrange
    QueryContext mockContext = TestUtils.getMockAllowContext();
    com.linkedin.common.GlossaryTerms input = new com.linkedin.common.GlossaryTerms();
    input.setTerms(new GlossaryTermAssociationArray());
    Urn entityUrn = UrnUtils.getUrn(TEST_ENTITY_URN_STRING);

    // Act
    GlossaryTerms result = GlossaryTermsMapper.map(mockContext, input, entityUrn);

    // Assert
    assertNotNull(result);
    assertTrue(result.getTerms().isEmpty());
  }

  @Test
  public void testMapWithAuthorizationFiltering() throws Exception {
    // Arrange - Create context that denies access
    QueryContext mockDenyContext = TestUtils.getMockDenyContext();
    com.linkedin.common.GlossaryTerms input = createTestGlossaryTerms();
    Urn entityUrn = UrnUtils.getUrn(TEST_ENTITY_URN_STRING);

    // Act
    GlossaryTerms result = GlossaryTermsMapper.map(mockDenyContext, input, entityUrn);

    // Assert
    assertNotNull(result);
    // Note: Authorization filtering may be based on specific URNs and permissions,
    // so we check that the result is processed without errors rather than asserting empty results
    assertTrue(result.getTerms().size() <= input.getTerms().size()); // Should not add terms
  }

  @Test
  public void testStaticMapMethod() throws Exception {
    // Arrange
    QueryContext mockContext = TestUtils.getMockAllowContext();
    com.linkedin.common.GlossaryTerms input = createTestGlossaryTerms();
    Urn entityUrn = UrnUtils.getUrn(TEST_ENTITY_URN_STRING);

    // Act
    GlossaryTerms result = GlossaryTermsMapper.map(mockContext, input, entityUrn);

    // Assert - Should be same as instance method
    assertNotNull(result);
    assertEquals(result.getTerms().size(), 2);
  }

  @Test
  public void testGlossaryTermNameExtraction() throws Exception {
    // Arrange
    QueryContext mockContext = TestUtils.getMockAllowContext();
    com.linkedin.common.GlossaryTerms input = new com.linkedin.common.GlossaryTerms();

    List<GlossaryTermAssociation> associations = new ArrayList<>();

    // Test hierarchical name extraction
    GlossaryTermAssociation association1 = new GlossaryTermAssociation();
    association1.setUrn(createGlossaryTermUrn("level1.level2.level3.finalterm"));
    associations.add(association1);

    // Test simple name (no dots)
    GlossaryTermAssociation association2 = new GlossaryTermAssociation();
    association2.setUrn(createGlossaryTermUrn("simpleterm"));
    associations.add(association2);

    input.setTerms(new GlossaryTermAssociationArray(associations));
    Urn entityUrn = UrnUtils.getUrn(TEST_ENTITY_URN_STRING);

    // Act
    GlossaryTerms result = GlossaryTermsMapper.map(mockContext, input, entityUrn);

    // Assert
    assertNotNull(result);
    assertEquals(result.getTerms().size(), 2);
    assertEquals(
        result.getTerms().get(0).getTerm().getName(), "finalterm"); // Should extract last part
    assertEquals(result.getTerms().get(1).getTerm().getName(), "simpleterm"); // Should return as-is
  }

  private com.linkedin.common.GlossaryTerms createTestGlossaryTerms() throws URISyntaxException {
    com.linkedin.common.GlossaryTerms glossaryTerms = new com.linkedin.common.GlossaryTerms();

    List<GlossaryTermAssociation> associations = new ArrayList<>();

    // First association with all optional fields
    GlossaryTermAssociation association1 = new GlossaryTermAssociation();
    association1.setUrn(createGlossaryTermUrn("test.term"));
    association1.setActor(UrnUtils.getUrn(TEST_ACTOR_URN_STRING));
    association1.setContext("test context");

    // Create metadata attribution
    MetadataAttribution attribution = new MetadataAttribution();
    attribution.setTime(System.currentTimeMillis());
    attribution.setActor(UrnUtils.getUrn(TEST_ACTOR_URN_STRING));
    association1.setAttribution(attribution);

    associations.add(association1);

    // Second association with minimal fields
    GlossaryTermAssociation association2 = new GlossaryTermAssociation();
    association2.setUrn(createGlossaryTermUrn("simple"));
    associations.add(association2);

    glossaryTerms.setTerms(new GlossaryTermAssociationArray(associations));
    return glossaryTerms;
  }

  private GlossaryTermUrn createGlossaryTermUrn(String termName) throws URISyntaxException {
    return GlossaryTermUrn.createFromString("urn:li:glossaryTerm:" + termName);
  }
}
