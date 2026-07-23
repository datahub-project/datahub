package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.OriginType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ServiceAccountServiceTest {

  @Mock private EntityService<?> mockEntityService;

  private ServiceAccountService serviceAccountService;
  private OperationContext operationContext;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    serviceAccountService = new ServiceAccountService();
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testBuildServiceUserUrn() {
    // Arrange
    String issuer = "https://auth.example.com";
    String subject = "service-account-123";

    // Act
    String userUrn = serviceAccountService.buildServiceUserUrn(issuer, subject);

    // Assert
    assertNotNull(userUrn);
    assertTrue(userUrn.startsWith("__oauth_"));
    assertTrue(userUrn.contains("auth_example_com"));
    assertTrue(userUrn.contains("service-account-123"));
    assertEquals(userUrn, "__oauth_auth_example_com_service-account-123");
  }

  @Test
  public void testBuildServiceUserUrnWithComplexIssuer() {
    // Arrange
    String issuer = "https://my-sso.company.com/oauth2";
    String subject = "svc_datahub";

    // Act
    String userUrn = serviceAccountService.buildServiceUserUrn(issuer, subject);

    // Assert
    assertNotNull(userUrn);
    assertEquals(userUrn, "__oauth_my_sso_company_com_oauth2_svc_datahub");
  }

  @Test
  public void testCreateServiceAccountAspects() {
    // Arrange
    CorpuserUrn userUrn = new CorpuserUrn("test-service-account");
    String displayName = "Test Service Account";
    OriginType originType = OriginType.EXTERNAL;
    String externalType = "https://auth.example.com";

    // Act
    List<MetadataChangeProposal> aspects =
        serviceAccountService.createServiceAccountAspects(
            userUrn, displayName, originType, externalType);

    // Assert
    assertNotNull(aspects);
    assertEquals(aspects.size(), 3);

    // Verify CorpUserInfo aspect
    MetadataChangeProposal corpUserInfoMcp =
        aspects.stream()
            .filter(mcp -> CORP_USER_INFO_ASPECT_NAME.equals(mcp.getAspectName()))
            .findFirst()
            .orElse(null);
    assertNotNull(corpUserInfoMcp);
    assertEquals(corpUserInfoMcp.getEntityUrn(), userUrn);
    assertEquals(corpUserInfoMcp.getEntityType(), "corpuser");

    // Verify SubTypes aspect
    MetadataChangeProposal subTypesMcp =
        aspects.stream()
            .filter(mcp -> SUB_TYPES_ASPECT_NAME.equals(mcp.getAspectName()))
            .findFirst()
            .orElse(null);
    assertNotNull(subTypesMcp);

    // Verify Origin aspect
    MetadataChangeProposal originMcp =
        aspects.stream()
            .filter(mcp -> ORIGIN_ASPECT_NAME.equals(mcp.getAspectName()))
            .findFirst()
            .orElse(null);
    assertNotNull(originMcp);
  }

  @Test
  public void testCreateServiceAccountSuccess() {
    // Arrange
    String userId = "__oauth_auth_example_com_service123";
    String displayName = "Service Account: service123 @ https://auth.example.com";
    OriginType originType = OriginType.EXTERNAL;
    String externalType = "https://auth.example.com";

    CorpuserUrn expectedUrn = new CorpuserUrn(userId);

    // Mock that user doesn't exist
    when(mockEntityService.exists(eq(operationContext), eq(expectedUrn), eq(false)))
        .thenReturn(false);

    // Act
    boolean result =
        serviceAccountService.createServiceAccount(
            userId, displayName, originType, externalType, mockEntityService, operationContext);

    // Assert
    assertTrue(result);
    verify(mockEntityService, times(1)).exists(eq(operationContext), eq(expectedUrn), eq(false));
    verify(mockEntityService, times(1))
        .ingestAspects(eq(operationContext), any(AspectsBatch.class), eq(false), eq(true));
  }

  @Test
  public void testCreateServiceAccountAlreadyExists() {
    // Arrange
    String userId = "__oauth_auth_example_com_service123";
    String displayName = "Service Account: service123 @ https://auth.example.com";
    OriginType originType = OriginType.EXTERNAL;
    String externalType = "https://auth.example.com";

    CorpuserUrn expectedUrn = new CorpuserUrn(userId);

    // Mock that user already exists
    when(mockEntityService.exists(eq(operationContext), eq(expectedUrn), eq(false)))
        .thenReturn(true);

    // Act
    boolean result =
        serviceAccountService.createServiceAccount(
            userId, displayName, originType, externalType, mockEntityService, operationContext);

    // Assert
    assertFalse(result);
    verify(mockEntityService, times(1)).exists(eq(operationContext), eq(expectedUrn), eq(false));
    verify(mockEntityService, never())
        .ingestAspects(eq(operationContext), any(AspectsBatch.class), eq(false), eq(true));
  }

  @Test
  public void testEnsureServiceAccountExistsFromTokenInfo() {
    // Arrange
    String userId = "__oauth_auth_example_com_service123";
    String issuer = "https://auth.example.com";
    String subject = "service123";

    CorpuserUrn expectedUrn = new CorpuserUrn(userId);

    // Mock that user doesn't exist
    when(mockEntityService.exists(eq(operationContext), eq(expectedUrn), eq(false)))
        .thenReturn(false);

    // Act
    boolean result =
        serviceAccountService.ensureServiceAccountExists(
            userId, issuer, subject, mockEntityService, operationContext);

    // Assert
    assertTrue(result);
    verify(mockEntityService, times(1)).exists(eq(operationContext), eq(expectedUrn), eq(false));
    verify(mockEntityService, times(1))
        .ingestAspects(eq(operationContext), any(AspectsBatch.class), eq(false), eq(true));
  }

  @Test
  public void testEnsureServiceAccountExistsAlreadyExists() {
    // Arrange
    String userId = "__oauth_auth_example_com_service123";
    String issuer = "https://auth.example.com";
    String subject = "service123";

    CorpuserUrn expectedUrn = new CorpuserUrn(userId);

    // Mock that user already exists
    when(mockEntityService.exists(eq(operationContext), eq(expectedUrn), eq(false)))
        .thenReturn(true);

    // Act
    boolean result =
        serviceAccountService.ensureServiceAccountExists(
            userId, issuer, subject, mockEntityService, operationContext);

    // Assert
    assertFalse(result);
    verify(mockEntityService, times(1)).exists(eq(operationContext), eq(expectedUrn), eq(false));
    verify(mockEntityService, never())
        .ingestAspects(eq(operationContext), any(AspectsBatch.class), eq(false), eq(true));
  }

  @Test
  public void testEnsureServiceAccountExistsHandlesErrors() {
    // Arrange
    String userId = "__oauth_auth_example_com_service123";
    String issuer = "https://auth.example.com";
    String subject = "service123";

    CorpuserUrn expectedUrn = new CorpuserUrn(userId);

    // Mock that user doesn't exist
    when(mockEntityService.exists(eq(operationContext), eq(expectedUrn), eq(false)))
        .thenReturn(false);

    // Mock ingestion failure
    doThrow(new RuntimeException("Ingestion failed"))
        .when(mockEntityService)
        .ingestAspects(eq(operationContext), any(AspectsBatch.class), eq(false), eq(true));

    // Act
    boolean result =
        serviceAccountService.ensureServiceAccountExists(
            userId, issuer, subject, mockEntityService, operationContext);

    // Assert
    assertFalse(result); // Should return false on error but not throw exception
    verify(mockEntityService, times(1)).exists(eq(operationContext), eq(expectedUrn), eq(false));
    verify(mockEntityService, times(1))
        .ingestAspects(eq(operationContext), any(AspectsBatch.class), eq(false), eq(true));
  }

  @Test
  public void testCreateSystemAuditStamp() {
    // Act
    var auditStamp = serviceAccountService.createSystemAuditStamp();

    // Assert
    assertNotNull(auditStamp);
    assertNotNull(auditStamp.getTime());
    assertNotNull(auditStamp.getActor());
    assertTrue(auditStamp.getTime() > 0);
  }

  @Test
  public void testCreateMetadataChangeProposal() {
    // Arrange
    CorpuserUrn userUrn = new CorpuserUrn("test-user");
    CorpUserInfo corpUserInfo = new CorpUserInfo();
    corpUserInfo.setActive(true);
    corpUserInfo.setDisplayName("Test User");

    // Act
    MetadataChangeProposal mcp =
        serviceAccountService.createMetadataChangeProposal(
            userUrn, CORP_USER_INFO_ASPECT_NAME, corpUserInfo);

    // Assert
    assertNotNull(mcp);
    assertEquals(mcp.getEntityUrn(), userUrn);
    assertEquals(mcp.getEntityType(), "corpuser");
    assertEquals(mcp.getAspectName(), CORP_USER_INFO_ASPECT_NAME);
    assertNotNull(mcp.getAspect());
    assertNotNull(mcp.getChangeType());
  }

  @Test
  public void testUserIdUniquenessAcrossIssuers() {
    // Arrange
    String issuer1 = "https://auth.company1.com";
    String issuer2 = "https://auth.company2.com";
    String subject = "service-account";

    // Act
    String userId1 = serviceAccountService.buildServiceUserUrn(issuer1, subject);
    String userId2 = serviceAccountService.buildServiceUserUrn(issuer2, subject);

    // Assert
    assertNotNull(userId1);
    assertNotNull(userId2);
    assertNotEquals(userId1, userId2);
    assertTrue(userId1.contains("auth_company1_com"));
    assertTrue(userId2.contains("auth_company2_com"));
  }

  @Test
  public void testIssuerSanitization() {
    // Test various issuer formats are properly sanitized
    String subject = "test";

    // Test HTTPS URL
    String issuer1 = "https://auth.example.com/oauth2/v1";
    String result1 = serviceAccountService.buildServiceUserUrn(issuer1, subject);
    assertTrue(result1.contains("auth_example_com_oauth2_v1"));

    // Test HTTP URL
    String issuer2 = "http://localhost:8080/auth";
    String result2 = serviceAccountService.buildServiceUserUrn(issuer2, subject);
    assertTrue(result2.contains("localhost_8080_auth"));

    // Test special characters
    String issuer3 = "https://auth-server.example.com:443/oauth2";
    String result3 = serviceAccountService.buildServiceUserUrn(issuer3, subject);
    assertTrue(result3.contains("auth_server_example_com_443_oauth2"));
  }
}
