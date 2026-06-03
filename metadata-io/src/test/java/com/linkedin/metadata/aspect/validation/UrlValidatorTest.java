package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpGroupEditableInfo;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UrlValidatorTest {
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_GROUP_URN = UrnUtils.getUrn("urn:li:corpGroup:testGroup");

  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(UrlValidator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT", "UPDATE", "PATCH"))
          .supportedEntityAspectNames(
              List.of(
                  new AspectPluginConfig.EntityAspectName(
                      CORP_USER_ENTITY_NAME, CORP_USER_EDITABLE_INFO_ASPECT_NAME),
                  new AspectPluginConfig.EntityAspectName(
                      CORP_GROUP_ENTITY_NAME, CORP_GROUP_EDITABLE_INFO_ASPECT_NAME)))
          .build();

  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private AspectRetriever mockAspectRetriever;

  private EntityRegistry entityRegistry;
  private UrlValidator validator;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    entityRegistry = new TestEntityRegistry();
    validator = new UrlValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
  }

  @Test
  public void testValidHttpsUrl() {
    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("https://example.com/photo.png"));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Valid HTTPS URL should pass validation");
  }

  @Test
  public void testDefaultAvatarPathAllowed() {
    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("assets/platforms/default_avatar.png"));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Default avatar relative path should pass validation");
  }

  @Test
  public void testEmptyPictureLinkAllowed() {
    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url(""));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Empty pictureLink should pass validation (user clearing profile image)");
  }

  @Test
  public void testHttpUrlRejected() {
    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("http://example.com/photo.png"));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "HTTP URL should be rejected (only HTTPS allowed)");
  }

  @Test
  public void testJavascriptSchemeRejected() {
    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("javascript:alert(1)"));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "javascript: scheme should be rejected");
  }

  @Test
  public void testDataSchemeRejected() {
    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("data:image/png;base64,iVBORw0KGgo="));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "data: scheme should be rejected");
  }

  @Test
  public void testFileSchemeRejected() {
    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("file:///etc/passwd"));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "file: scheme should be rejected");
  }

  @Test
  public void testLocalhostRejected() {
    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("https://localhost/secret"));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "localhost should be rejected");
  }

  @Test
  public void testCloudMetadataEndpointRejected() {
    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("https://169.254.169.254/latest/meta-data/"));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Cloud metadata endpoint (169.254.169.254) should be rejected");
  }

  @Test
  public void testLoopbackIpRejected() {
    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("https://127.0.0.1/admin"));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Loopback IP (127.0.0.1) should be rejected");
  }

  @Test
  public void testPrivateNetworkIpRejected() {
    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("https://192.168.1.1/image.png"));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Private network IP (192.168.x.x) should be rejected");
  }

  @Test
  public void testCorpGroupEditableInfoValidated() {
    CorpGroupEditableInfo info = new CorpGroupEditableInfo();
    info.setPictureLink(new Url("http://example.com/photo.png"));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_GROUP_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_GROUP_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_GROUP_URN.getEntityType())
                                .getAspectSpec(CORP_GROUP_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Corp group pictureLink should also be validated (HTTP rejected)");
  }

  @Test
  public void testIsInternalHost() {
    assertTrue(UrlValidator.isInternalHost("localhost"));
    assertTrue(UrlValidator.isInternalHost("127.0.0.1"));
    assertTrue(UrlValidator.isInternalHost("::1"));
    assertTrue(UrlValidator.isInternalHost("169.254.169.254"));
    assertTrue(UrlValidator.isInternalHost("192.168.1.1"));
    assertTrue(UrlValidator.isInternalHost("10.0.0.1"));
    assertFalse(UrlValidator.isInternalHost("example.com"));
    assertFalse(UrlValidator.isInternalHost("github.com"));
  }

  // --- Configuration tests ---

  @Test
  public void testAllowHttpWhenConfigured() {
    UrlValidator httpValidator = new UrlValidator();
    httpValidator.setConfig(TEST_PLUGIN_CONFIG);
    httpValidator.setAllowHttp(true);

    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("http://example.com/photo.png"));

    assertEquals(
        httpValidator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "HTTP URL should pass when allowHttp is true");
  }

  @Test
  public void testExtraDenyHostsBlocked() {
    UrlValidator customValidator = new UrlValidator();
    customValidator.setConfig(TEST_PLUGIN_CONFIG);
    customValidator.setExtraDenyHostsList(List.of("blocked.example.com", "evil.corp"));

    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("https://blocked.example.com/photo.png"));

    assertEquals(
        customValidator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Extra deny host should be blocked");
  }

  @Test
  public void testExtraDenyHostsCaseInsensitive() {
    UrlValidator customValidator = new UrlValidator();
    customValidator.setConfig(TEST_PLUGIN_CONFIG);
    customValidator.setExtraDenyHostsList(List.of("BLOCKED.Example.COM"));

    CorpUserEditableInfo info = new CorpUserEditableInfo();
    info.setPictureLink(new Url("https://blocked.example.com/photo.png"));

    assertEquals(
        customValidator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_EDITABLE_INFO_ASPECT_NAME))
                        .recordTemplate(info)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Extra deny hosts should be case-insensitive");
  }
}
