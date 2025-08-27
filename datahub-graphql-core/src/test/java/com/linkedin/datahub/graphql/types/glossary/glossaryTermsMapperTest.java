package com.linkedin.datahub.graphql.types.glossary;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import java.util.List;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class glossaryTermsMapperTest {

  private GlossaryTerms glossaryTerms;
  private static final String TEST_TERM_URN = "urn:li:glossaryTerm:test-glossary";
  private static final String TEST_ASSOCIATED_ENTITY_URN =
      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,dataset-1,DEV),schemaField-1)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:testUser";
  private static final long TEST_TIME = 1234567890L;
  QueryContext mockContext = mock(QueryContext.class);
  MockedStatic<AuthorizationUtils> authUtilsMock = Mockito.mockStatic(AuthorizationUtils.class);

  @BeforeMethod
  public void setup() throws Exception {
    glossaryTerms = new GlossaryTerms();

    GlossaryTermAssociation glossaryTermAssociation = new GlossaryTermAssociation();
    glossaryTermAssociation.setActor(Urn.createFromString(TEST_ACTOR_URN));
    glossaryTermAssociation.setUrn(GlossaryTermUrn.createFromString(TEST_TERM_URN));

    MetadataAttribution metadataAttribution = new MetadataAttribution();
    metadataAttribution.setActor(Urn.createFromString(TEST_ACTOR_URN));
    metadataAttribution.setTime(TEST_TIME);
    glossaryTermAssociation.setAttribution(metadataAttribution);

    GlossaryTermAssociationArray glossaryTermAssociations =
        new GlossaryTermAssociationArray(List.of(glossaryTermAssociation));

    glossaryTerms.setTerms(glossaryTermAssociations);

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(Urn.createFromString(TEST_ACTOR_URN));
    auditStamp.setTime(TEST_TIME);

    glossaryTerms.setAuditStamp(auditStamp);

    authUtilsMock
        .when(
            () ->
                AuthorizationUtils.canView(
                    Mockito.any(), Mockito.eq(GlossaryTermUrn.createFromString(TEST_TERM_URN))))
        .thenReturn(true);
  }

  @Test
  public void testGlossaryTermsMapper() throws Exception {
    // Create an entity URN to associate with the glossary terms
    Urn entityUrn = Urn.createFromString(TEST_ASSOCIATED_ENTITY_URN);

    // Call the mapper
    com.linkedin.datahub.graphql.generated.GlossaryTerms result =
        GlossaryTermsMapper.map(mockContext, glossaryTerms, entityUrn);

    // Verify the result
    assertNotNull(result, "Mapped result should not be null");
    assertNotNull(result.getTerms(), "Terms list should not be null");
    assertEquals(result.getTerms().size(), 1, "Should have exactly one term association");

    // Verify the term association
    com.linkedin.datahub.graphql.generated.GlossaryTermAssociation association =
        result.getTerms().get(0);
    assertNotNull(association, "Term association should not be null");
    assertNotNull(association.getTerm(), "Term should not be null");
    assertEquals(association.getTerm().getUrn(), TEST_TERM_URN, "Term URN should match");
    assertEquals(
        association.getTerm().getType(),
        EntityType.GLOSSARY_TERM,
        "Term type should be GLOSSARY_TERM");
    assertEquals(
        association.getAssociatedUrn(), TEST_ASSOCIATED_ENTITY_URN, "Associated URN should match");

    // Verify actor
    assertNotNull(association.getActor(), "Actor should not be null");
    assertEquals(association.getActor().getUrn(), TEST_ACTOR_URN, "Actor URN should match");
    assertEquals(
        association.getActor().getType(), EntityType.CORP_USER, "Actor type should be CORP_USER");

    // Verify attribution
    assertNotNull(association.getAttribution(), "Attribution should not be null");
    assertNotNull(association.getAttribution().getActor(), "Attribution actor should not be null");
    assertEquals(
        association.getAttribution().getActor().getUrn(),
        TEST_ACTOR_URN,
        "Attribution actor URN should match");
    assertEquals(
        association.getAttribution().getTime(), TEST_TIME, "Attribution time should match");
  }

  @Test
  public void testGlossaryTermsMapperWithMultipleTerms() throws Exception {
    // Create a second glossary term association
    GlossaryTermAssociation secondAssociation = new GlossaryTermAssociation();
    String secondTermUrn = "urn:li:glossaryTerm:second-glossary";
    secondAssociation.setActor(Urn.createFromString(TEST_ACTOR_URN));
    secondAssociation.setUrn(GlossaryTermUrn.createFromString(secondTermUrn));

    MetadataAttribution secondAttribution = new MetadataAttribution();
    secondAttribution.setActor(Urn.createFromString(TEST_ACTOR_URN));
    secondAttribution.setTime(TEST_TIME + 1000);
    secondAssociation.setAttribution(secondAttribution);

    // Add the second association to the terms array
    GlossaryTermAssociationArray updatedAssociations = new GlossaryTermAssociationArray();
    updatedAssociations.add(glossaryTerms.getTerms().get(0));
    updatedAssociations.add(secondAssociation);
    glossaryTerms.setTerms(updatedAssociations);

    // Create an entity URN to associate with the glossary terms
    Urn entityUrn = Urn.createFromString(TEST_ASSOCIATED_ENTITY_URN);

    authUtilsMock
        .when(
            () ->
                AuthorizationUtils.canView(
                    Mockito.any(), Mockito.eq(GlossaryTermUrn.createFromString(secondTermUrn))))
        .thenReturn(true);

    // Call the mapper
    com.linkedin.datahub.graphql.generated.GlossaryTerms result =
        GlossaryTermsMapper.map(mockContext, glossaryTerms, entityUrn);

    // Verify the result has both terms
    assertNotNull(result, "Mapped result should not be null");
    assertNotNull(result.getTerms(), "Terms list should not be null");
    assertEquals(result.getTerms().size(), 2, "Should have exactly two term associations");

    // Verify the first term association
    com.linkedin.datahub.graphql.generated.GlossaryTermAssociation firstAssociation =
        result.getTerms().get(0);
    assertEquals(firstAssociation.getTerm().getUrn(), TEST_TERM_URN, "First term URN should match");

    // Verify the second term association
    com.linkedin.datahub.graphql.generated.GlossaryTermAssociation mappedSecondAssociation =
        result.getTerms().get(1);
    assertEquals(
        mappedSecondAssociation.getTerm().getUrn(), secondTermUrn, "Second term URN should match");
    assertEquals(
        mappedSecondAssociation.getAssociatedUrn(),
        TEST_ASSOCIATED_ENTITY_URN,
        "Second term associated URN should match");
    assertEquals(
        mappedSecondAssociation.getAttribution().getTime(),
        TEST_TIME + 1000,
        "Second term attribution time should match");
  }

  @Test
  public void testGlossaryTermsMapperWithAuthorizationFiltering() throws Exception {
    // Create a second glossary term association
    GlossaryTermAssociation secondAssociation = new GlossaryTermAssociation();
    String secondTermUrn = "urn:li:glossaryTerm:second-glossary";
    secondAssociation.setActor(Urn.createFromString(TEST_ACTOR_URN));
    secondAssociation.setUrn(GlossaryTermUrn.createFromString(secondTermUrn));

    // Add the second association to the terms array
    GlossaryTermAssociationArray updatedAssociations = new GlossaryTermAssociationArray();
    updatedAssociations.add(glossaryTerms.getTerms().get(0));
    updatedAssociations.add(secondAssociation);
    glossaryTerms.setTerms(updatedAssociations);

    // Create an entity URN to associate with the glossary terms
    Urn entityUrn = Urn.createFromString(TEST_ASSOCIATED_ENTITY_URN);

    // deny access to the second term
    authUtilsMock
        .when(
            () ->
                AuthorizationUtils.canView(
                    Mockito.any(), Mockito.eq(GlossaryTermUrn.createFromString(secondTermUrn))))
        .thenReturn(false);

    // Call the mapper with the mock context
    com.linkedin.datahub.graphql.generated.GlossaryTerms result =
        GlossaryTermsMapper.map(mockContext, glossaryTerms, entityUrn);

    // Verify the result only contains the authorized term
    assertNotNull(result, "Mapped result should not be null");
    assertNotNull(result.getTerms(), "Terms list should not be null");
    assertEquals(
        result.getTerms().size(), 1, "Should have exactly one term association after filtering");

    // Verify it's the first term that was authorized
    com.linkedin.datahub.graphql.generated.GlossaryTermAssociation association =
        result.getTerms().get(0);
    assertEquals(association.getTerm().getUrn(), TEST_TERM_URN, "Authorized term URN should match");
  }
}
