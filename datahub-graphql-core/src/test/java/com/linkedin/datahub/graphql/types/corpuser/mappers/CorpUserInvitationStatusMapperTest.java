package com.linkedin.datahub.graphql.types.corpuser.mappers;

import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.CorpUserInvitationStatus;
import com.linkedin.datahub.graphql.generated.InvitationStatus;
import org.testng.annotations.Test;

public class CorpUserInvitationStatusMapperTest {

  private static final String TEST_ACTOR_URN = "urn:li:corpuser:testUser";
  private static final String TEST_ROLE_URN = "urn:li:dataHubRole:Admin";
  private static final String TEST_INVITATION_TOKEN = "abc123def456";
  private static final long TEST_CREATED_TIME = 1234567890L;
  private static final long TEST_UPDATED_TIME = 1234567900L;

  @Test
  public void testMapWithAllFields() throws Exception {
    // Create test input
    final com.linkedin.identity.CorpUserInvitationStatus input =
        new com.linkedin.identity.CorpUserInvitationStatus();

    input.setStatus(com.linkedin.identity.InvitationStatus.SENT);
    input.setRole(Urn.createFromString(TEST_ROLE_URN));
    input.setInvitationToken(TEST_INVITATION_TOKEN);

    // Set created audit stamp
    final AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_CREATED_TIME);
    createdStamp.setActor(Urn.createFromString(TEST_ACTOR_URN));
    input.setCreated(createdStamp);

    // Set last updated audit stamp
    final AuditStamp lastUpdatedStamp = new AuditStamp();
    lastUpdatedStamp.setTime(TEST_UPDATED_TIME);
    lastUpdatedStamp.setActor(Urn.createFromString(TEST_ACTOR_URN));
    input.setLastUpdated(lastUpdatedStamp);

    // Map to GraphQL type
    CorpUserInvitationStatus result = CorpUserInvitationStatusMapper.map(null, input);

    // Verify all fields are mapped correctly
    assertNotNull(result);
    assertEquals(result.getStatus(), InvitationStatus.SENT);
    assertEquals(result.getRole(), TEST_ROLE_URN);
    assertEquals(result.getInvitationToken(), TEST_INVITATION_TOKEN);

    // Verify created audit stamp
    assertNotNull(result.getCreated());
    assertEquals(result.getCreated().getTime().longValue(), TEST_CREATED_TIME);
    assertEquals(result.getCreated().getActor(), TEST_ACTOR_URN);

    // Verify last updated audit stamp
    assertNotNull(result.getLastUpdated());
    assertEquals(result.getLastUpdated().getTime().longValue(), TEST_UPDATED_TIME);
    assertEquals(result.getLastUpdated().getActor(), TEST_ACTOR_URN);
  }

  @Test
  public void testMapWithRequiredFieldsOnly() throws Exception {
    final com.linkedin.identity.CorpUserInvitationStatus input =
        new com.linkedin.identity.CorpUserInvitationStatus();

    input.setStatus(com.linkedin.identity.InvitationStatus.ACCEPTED);
    input.setInvitationToken(TEST_INVITATION_TOKEN);

    // Set created audit stamp (required)
    final AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_CREATED_TIME);
    createdStamp.setActor(Urn.createFromString(TEST_ACTOR_URN));
    input.setCreated(createdStamp);

    // Set last updated audit stamp (required)
    final AuditStamp lastUpdatedStamp = new AuditStamp();
    lastUpdatedStamp.setTime(TEST_UPDATED_TIME);
    lastUpdatedStamp.setActor(Urn.createFromString(TEST_ACTOR_URN));
    input.setLastUpdated(lastUpdatedStamp);

    CorpUserInvitationStatus result = CorpUserInvitationStatusMapper.map(null, input);

    assertNotNull(result);
    assertEquals(result.getStatus(), InvitationStatus.ACCEPTED);
    assertNull(result.getRole()); // Optional field not set
    assertEquals(result.getInvitationToken(), TEST_INVITATION_TOKEN);

    assertNotNull(result.getCreated());
    assertEquals(result.getCreated().getTime().longValue(), TEST_CREATED_TIME);

    assertNotNull(result.getLastUpdated());
    assertEquals(result.getLastUpdated().getTime().longValue(), TEST_UPDATED_TIME);
  }

  @Test
  public void testMapAllStatusValues() throws Exception {
    // Test each enum value conversion
    testStatusMapping(com.linkedin.identity.InvitationStatus.SENT, InvitationStatus.SENT);
    testStatusMapping(com.linkedin.identity.InvitationStatus.ACCEPTED, InvitationStatus.ACCEPTED);
    testStatusMapping(com.linkedin.identity.InvitationStatus.REVOKED, InvitationStatus.REVOKED);
    testStatusMapping(
        com.linkedin.identity.InvitationStatus.SUGGESTION_DISMISSED,
        InvitationStatus.SUGGESTION_DISMISSED);
  }

  private void testStatusMapping(
      com.linkedin.identity.InvitationStatus inputStatus, InvitationStatus expectedStatus)
      throws Exception {

    final com.linkedin.identity.CorpUserInvitationStatus input =
        new com.linkedin.identity.CorpUserInvitationStatus();

    input.setStatus(inputStatus);
    input.setInvitationToken(TEST_INVITATION_TOKEN);

    // Set required audit stamps
    final AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_CREATED_TIME);
    createdStamp.setActor(Urn.createFromString(TEST_ACTOR_URN));
    input.setCreated(createdStamp);

    final AuditStamp lastUpdatedStamp = new AuditStamp();
    lastUpdatedStamp.setTime(TEST_UPDATED_TIME);
    lastUpdatedStamp.setActor(Urn.createFromString(TEST_ACTOR_URN));
    input.setLastUpdated(lastUpdatedStamp);

    CorpUserInvitationStatus result = CorpUserInvitationStatusMapper.map(null, input);

    assertNotNull(result);
    assertEquals(result.getStatus(), expectedStatus);
  }

  @Test
  public void testMapWithoutOptionalRole() throws Exception {
    final com.linkedin.identity.CorpUserInvitationStatus input =
        new com.linkedin.identity.CorpUserInvitationStatus();

    input.setStatus(com.linkedin.identity.InvitationStatus.SENT);
    input.setInvitationToken(TEST_INVITATION_TOKEN);

    // AuditStamp requires actor field
    final AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_CREATED_TIME);
    createdStamp.setActor(Urn.createFromString(TEST_ACTOR_URN));
    input.setCreated(createdStamp);

    final AuditStamp lastUpdatedStamp = new AuditStamp();
    lastUpdatedStamp.setTime(TEST_UPDATED_TIME);
    lastUpdatedStamp.setActor(Urn.createFromString(TEST_ACTOR_URN));
    input.setLastUpdated(lastUpdatedStamp);

    CorpUserInvitationStatus result = CorpUserInvitationStatusMapper.map(null, input);

    assertNotNull(result);
    assertEquals(result.getStatus(), InvitationStatus.SENT);
    assertNull(result.getRole());
    assertEquals(result.getInvitationToken(), TEST_INVITATION_TOKEN);
  }
}
