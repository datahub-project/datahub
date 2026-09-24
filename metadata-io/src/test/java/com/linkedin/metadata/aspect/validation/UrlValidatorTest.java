package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Embed;
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
import com.linkedin.test.metadata.aspect.batch.TestPatchMCP;
import java.util.List;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UrlValidatorTest {
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_GROUP_URN = UrnUtils.getUrn("urn:li:corpGroup:testGroup");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)");
  private static final Urn TEST_CHART_URN = UrnUtils.getUrn("urn:li:chart:(looker,my_chart)");
  private static final Urn TEST_DASHBOARD_URN =
      UrnUtils.getUrn("urn:li:dashboard:(looker,my_dashboard)");

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
                      CORP_GROUP_ENTITY_NAME, CORP_GROUP_EDITABLE_INFO_ASPECT_NAME),
                  new AspectPluginConfig.EntityAspectName(DATASET_ENTITY_NAME, EMBED_ASPECT_NAME),
                  new AspectPluginConfig.EntityAspectName(CHART_ENTITY_NAME, EMBED_ASPECT_NAME),
                  new AspectPluginConfig.EntityAspectName(
                      DASHBOARD_ENTITY_NAME, EMBED_ASPECT_NAME)))
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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

  // --- Embed renderUrl tests (scheme-allowlist only: http/https allowed, internal hosts allowed)
  // ---

  private long validateEmbedRenderUrl(String renderUrl) {
    return validateEmbedRenderUrl(TEST_DATASET_URN, renderUrl);
  }

  private long validateEmbedRenderUrl(Urn urn, String renderUrl) {
    Embed embed = new Embed();
    embed.setRenderUrl(renderUrl);
    return validator
        .validateProposed(
            OperationFingerprint.EMPTY,
            Set.of(
                TestMCP.builder()
                    .changeType(ChangeType.UPSERT)
                    .urn(urn)
                    .entitySpec(entityRegistry.getEntitySpec(urn.getEntityType()))
                    .aspectSpec(
                        entityRegistry
                            .getEntitySpec(urn.getEntityType())
                            .getAspectSpec(EMBED_ASPECT_NAME))
                    .recordTemplate(embed)
                    .build()),
            mockRetrieverContext,
            null)
        .count();
  }

  private long validateEmbedPatch(Urn urn, String renderUrl) {
    String ops = "[{\"op\":\"add\",\"path\":\"/renderUrl\",\"value\":\"" + renderUrl + "\"}]";
    return validator
        .validateProposed(
            OperationFingerprint.EMPTY,
            Set.of(TestPatchMCP.of(urn, EMBED_ASPECT_NAME, ops)),
            mockRetrieverContext,
            null)
        .count();
  }

  @Test
  public void testEmbedJavascriptSchemeRejected() {
    assertEquals(
        validateEmbedRenderUrl("javascript:alert(document.cookie)"),
        1,
        "javascript: renderUrl should be rejected (stored XSS guard)");
  }

  @Test
  public void testEmbedHttpsAccepted() {
    assertEquals(
        validateEmbedRenderUrl("https://bi.example.com/dashboard/1"),
        0,
        "HTTPS renderUrl should be accepted");
  }

  @Test
  public void testEmbedHttpInternalHostAccepted() {
    // Unlike pictureLink validation, embeds intentionally allow HTTP and internal hosts so that
    // self-hosted/internal BI tools can be embedded. A private IP that pictureLink would reject
    // must pass here.
    assertEquals(
        validateEmbedRenderUrl("http://192.168.1.10/dashboard"),
        0,
        "HTTP renderUrl to an internal host should be accepted for embeds");
  }

  @Test
  public void testEmbedBlankRenderUrlAllowed() {
    assertEquals(validateEmbedRenderUrl(""), 0, "Blank renderUrl (clearing the embed) is allowed");
  }

  @Test
  public void testEmbedFtpAndMailtoAccepted() {
    // The embed allowlist mirrors the frontend safeUrl allowlist (http/https/ftp/mailto) so the
    // write-time check and the render-time guard agree. ftp:/mailto: are inert (not XSS vectors).
    assertEquals(validateEmbedRenderUrl("ftp://files.example.com/report"), 0, "ftp: is allowed");
    assertEquals(validateEmbedRenderUrl("mailto:team@example.com"), 0, "mailto: is allowed");
  }

  @Test
  public void testEmbedDataSchemeRejected() {
    assertEquals(
        validateEmbedRenderUrl("data:text/html,<script>alert(1)</script>"),
        1,
        "data: renderUrl should be rejected (stored XSS guard)");
  }

  @Test
  public void testEmbedRenderUrlValidatedForChartAndDashboard() {
    // The validator is registered for chart and dashboard embeds too, not just dataset.
    assertEquals(
        validateEmbedRenderUrl(TEST_CHART_URN, "javascript:alert(1)"),
        1,
        "javascript: renderUrl on a chart embed should be rejected");
    assertEquals(
        validateEmbedRenderUrl(TEST_CHART_URN, "https://bi.example.com/chart/1"),
        0,
        "HTTPS renderUrl on a chart embed should be accepted");
    assertEquals(
        validateEmbedRenderUrl(TEST_DASHBOARD_URN, "javascript:alert(1)"),
        1,
        "javascript: renderUrl on a dashboard embed should be rejected");
    assertEquals(
        validateEmbedRenderUrl(TEST_DASHBOARD_URN, "https://bi.example.com/dash/1"),
        0,
        "HTTPS renderUrl on a dashboard embed should be accepted");
  }

  @Test
  public void testEmbedPatchJavascriptSchemeRejected() {
    // A PATCH carries only the delta (getAspect is null), so this covers the generic PATCH path
    // that would otherwise bypass the stored-XSS guard.
    assertEquals(
        validateEmbedPatch(TEST_DATASET_URN, "javascript:alert(document.cookie)"),
        1,
        "javascript: renderUrl set via PATCH should be rejected");
  }

  @Test
  public void testEmbedPatchHttpsAccepted() {
    assertEquals(
        validateEmbedPatch(TEST_DATASET_URN, "https://bi.example.com/dashboard/1"),
        0,
        "HTTPS renderUrl set via PATCH should be accepted");
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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
                OperationFingerprint.EMPTY,
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
