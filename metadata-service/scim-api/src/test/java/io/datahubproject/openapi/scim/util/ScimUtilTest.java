package io.datahubproject.openapi.scim.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.Origin;
import com.linkedin.common.OriginType;
import com.linkedin.entity.EnvelopedAspect;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ScimUtilTest {

  @Mock private EnvelopedAspect mockEnvelopedAspect;

  @Test
  void testIsScimCreatedEntity_ExternalTypeOkta() {
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("OKTA");
    assertFalse(ScimUtil.isScimCreatedEntity(origin));
  }

  @Test
  void testIsScimCreatedEntity_ExternalTypeScimClientPrefix() {
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("SCIM_client_");
    assertTrue(ScimUtil.isScimCreatedEntity(origin));
  }

  @Test
  void testIsScimCreatedEntity_ExternalTypeScimClientWithId() {
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("SCIM_client_user123");
    assertTrue(ScimUtil.isScimCreatedEntity(origin));
  }

  @Test
  void testIsScimCreatedEntity_ExternalTypeScimClientWithNullString() {
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("SCIM_client_null");
    assertTrue(ScimUtil.isScimCreatedEntity(origin));
  }

  @Test
  void testExtractScimExternalId_ScimClientWithValidId() {
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("SCIM_client_user123");
    assertEquals("user123", ScimUtil.extractScimExternalId(origin));
  }

  @Test
  void testExtractScimExternalId_ScimClientWithNullString() {
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("SCIM_client_null");
    assertNull(ScimUtil.extractScimExternalId(origin));
  }

  @Test
  void testExtractScimExternalId_ScimClientWithComplexId() {
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("SCIM_client_user@example.com");
    assertEquals("user@example.com", ScimUtil.extractScimExternalId(origin));
  }

  @Test
  void testExtractScimExternalId_WithSpecialCharacters() {
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("SCIM_client_user-123_test@domain.com");
    assertEquals("user-123_test@domain.com", ScimUtil.extractScimExternalId(origin));
  }

  @Test
  void testExtractScimExternalId_WithUnicodeCharacters() {
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("SCIM_client_用户123");
    assertEquals("用户123", ScimUtil.extractScimExternalId(origin));
  }

  @Test
  void testExtractOriginFromAspects_EmptyList() {
    assertNull(ScimUtil.extractOriginFromAspects(Collections.emptyList()));
  }

  @Test
  void testExtractOriginFromAspects_NoOriginAspect() {
    EnvelopedAspect aspect1 = mock(EnvelopedAspect.class);
    when(aspect1.getName()).thenReturn("corpUserKey");

    EnvelopedAspect aspect2 = mock(EnvelopedAspect.class);
    when(aspect2.getName()).thenReturn("corpUserInfo");

    List<EnvelopedAspect> aspects = Arrays.asList(aspect1, aspect2);
    assertNull(ScimUtil.extractOriginFromAspects(aspects));
  }

  @Test
  void testLogNonScimEntityWithUnexpectedType_WithExternalType() {
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("OKTA");

    // This should not throw an exception
    assertDoesNotThrow(
        () ->
            ScimUtil.logNonScimEntityWithUnexpectedType(
                "urn:li:corpuser:test", origin, "Skipping"));
  }

  @Test
  void testLogNonScimEntityWithUnexpectedType_WithDifferentAction() {
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("LDAP");

    // This should not throw an exception
    assertDoesNotThrow(
        () ->
            ScimUtil.logNonScimEntityWithUnexpectedType(
                "urn:li:corpuser:test", origin, "Attempted to access"));
  }

  @Test
  void testScimUtilWorkflow_CompleteFlow() {
    // Test the complete workflow: create origin, check if SCIM, extract ID
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("SCIM_client_integration_test_user");

    // Step 1: Check if it's a SCIM entity
    assertTrue(ScimUtil.isScimCreatedEntity(origin));

    // Step 2: Extract the external ID
    String externalId = ScimUtil.extractScimExternalId(origin);
    assertEquals("integration_test_user", externalId);

    // Step 3: Logging should work without issues
    assertDoesNotThrow(
        () ->
            ScimUtil.logNonScimEntityWithUnexpectedType(
                "urn:li:corpuser:test", origin, "Processing"));
  }

  @Test
  void testScimUtilWorkflow_NonScimEntity() {
    // Test workflow with non-SCIM entity
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType("OKTA");

    // Step 1: Check if it's a SCIM entity
    assertFalse(ScimUtil.isScimCreatedEntity(origin));

    // Step 2: Extract the external ID should return null
    String externalId = ScimUtil.extractScimExternalId(origin);
    assertNull(externalId);

    // Step 3: Logging should work and log a warning
    assertDoesNotThrow(
        () ->
            ScimUtil.logNonScimEntityWithUnexpectedType(
                "urn:li:corpuser:okta_user", origin, "Skipping"));
  }
}
