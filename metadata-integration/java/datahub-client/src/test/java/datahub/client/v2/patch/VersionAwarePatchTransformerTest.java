package datahub.client.v2.patch;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.builder.GlobalTagsPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.v2.config.ServerConfig;
import datahub.client.v2.operations.EntityClient;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class VersionAwarePatchTransformerTest {

  @Mock private EntityClient mockEntityClient;

  private VersionAwarePatchTransformer transformer;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
    transformer = new VersionAwarePatchTransformer(mockEntityClient);
  }

  /** Helper to create a ServerConfig for testing with a specific version and environment. */
  private ServerConfig createServerConfig(String version, String serverEnv) {
    ObjectNode config = OBJECT_MAPPER.createObjectNode();

    // Create versions node
    ObjectNode versions = OBJECT_MAPPER.createObjectNode();
    ObjectNode datahubVersion = OBJECT_MAPPER.createObjectNode();
    datahubVersion.put("version", version);
    versions.set("acryldata/datahub", datahubVersion);
    config.set("versions", versions);

    // Create datahub node with serverEnv
    ObjectNode datahub = OBJECT_MAPPER.createObjectNode();
    datahub.put("serverEnv", serverEnv);
    config.set("datahub", datahub);

    return ServerConfig.fromJson(config);
  }

  /** Helper to create a Core ServerConfig with specific version. */
  private ServerConfig createCoreServerConfig(String version) {
    return createServerConfig(version, "core");
  }

  /** Helper to create a Cloud ServerConfig with specific version. */
  private ServerConfig createCloudServerConfig(String version) {
    return createServerConfig(version, "cloud");
  }

  @Test
  public void testTransform_withoutServerConfig_passesThrough() throws Exception {
    // Create an ownership patch
    MetadataChangeProposal ownershipPatch = createOwnershipPatch();
    List<MetadataChangeProposal> patches = new ArrayList<>();
    patches.add(ownershipPatch);

    // Transform with null server config
    List<datahub.client.v2.patch.TransformResult> results = transformer.transform(patches, null);

    // Should pass through unchanged
    assertEquals(1, results.size());
    assertEquals(ownershipPatch, results.get(0).getMcp());
    assertFalse(results.get(0).hasRetryFunction()); // No retry function for non-retryable patches

    // Should not call entity client
    verify(mockEntityClient, never()).getAspect(any(), any());
  }

  @Test
  public void testTransform_coreV131_noTransformationNeeded() throws Exception {
    // Create an ownership patch
    MetadataChangeProposal ownershipPatch = createOwnershipPatch();
    List<MetadataChangeProposal> patches = new ArrayList<>();
    patches.add(ownershipPatch);

    // Create server config for v1.3.1 (supports all patches)
    ServerConfig serverConfig = createCoreServerConfig("v1.3.1");

    // Transform
    List<datahub.client.v2.patch.TransformResult> results =
        transformer.transform(patches, serverConfig);

    // Should pass through unchanged
    assertEquals(1, results.size());
    assertEquals(ownershipPatch, results.get(0).getMcp());
    assertFalse(results.get(0).hasRetryFunction());

    // Should not call entity client
    verify(mockEntityClient, never()).getAspect(any(), any());
  }

  @Test
  public void testTransform_coreV130_tagPatchNotTransformed() throws Exception {
    // Tag patches should NOT be transformed because GlobalTagsTemplate exists on all servers
    MetadataChangeProposal tagPatch = createTagPatch();
    List<MetadataChangeProposal> patches = new ArrayList<>();
    patches.add(tagPatch);

    // Create server config for v1.3.0
    ServerConfig serverConfig = createCoreServerConfig("v1.3.0");

    // Transform
    List<datahub.client.v2.patch.TransformResult> results =
        transformer.transform(patches, serverConfig);

    // Tag patch should pass through unchanged (GlobalTagsTemplate always existed)
    assertEquals(1, results.size());
    assertEquals(ChangeType.PATCH, results.get(0).getMcp().getChangeType());
    assertEquals("globalTags", results.get(0).getMcp().getAspectName());
    assertFalse(results.get(0).hasRetryFunction());

    // Should not call entity client
    verify(mockEntityClient, never()).getAspect(any(), any());
  }

  @Test
  public void testTransform_cloudVersion_noTransformationNeeded() throws Exception {
    // Create an ownership patch
    MetadataChangeProposal ownershipPatch = createOwnershipPatch();
    List<MetadataChangeProposal> patches = new ArrayList<>();
    patches.add(ownershipPatch);

    // Create server config for Cloud (currently assumes support)
    ServerConfig serverConfig = createCloudServerConfig("v0.1.0");

    // Transform
    List<datahub.client.v2.patch.TransformResult> results =
        transformer.transform(patches, serverConfig);

    // Should pass through unchanged (Cloud support assumed for now)
    assertEquals(1, results.size());
    assertEquals(ownershipPatch, results.get(0).getMcp());
    assertFalse(results.get(0).hasRetryFunction());

    // Should not call entity client
    verify(mockEntityClient, never()).getAspect(any(), any());
  }

  @Test
  public void testTransform_nonTagPatch_passesThrough() throws Exception {
    // Create a non-tag patch (ownership patch)
    MetadataChangeProposal ownershipPatch = new MetadataChangeProposal();
    ownershipPatch.setEntityType("dataset");
    ownershipPatch.setEntityUrn(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)"));
    ownershipPatch.setAspectName("ownership");
    ownershipPatch.setChangeType(ChangeType.PATCH);

    List<MetadataChangeProposal> patches = new ArrayList<>();
    patches.add(ownershipPatch);

    // Create server config for v1.3.0 (tag patches broken, but this isn't a tag patch)
    ServerConfig serverConfig = createCoreServerConfig("v1.3.0");

    // Transform
    List<datahub.client.v2.patch.TransformResult> results =
        transformer.transform(patches, serverConfig);

    // Should pass through unchanged (not a tag patch)
    assertEquals(1, results.size());
    assertEquals(ownershipPatch, results.get(0).getMcp());
    assertFalse(results.get(0).hasRetryFunction());

    // Should not call entity client
    verify(mockEntityClient, never()).getAspect(any(), any());
  }

  @Test
  public void testTransform_mixedPatches_onlyNonTagsPassThrough() throws Exception {
    // Create mixed patches - tag and ownership
    Urn datasetUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");

    MetadataChangeProposal tagPatch = createTagPatchWithUrn(datasetUrn);

    MetadataChangeProposal ownershipPatch = new MetadataChangeProposal();
    ownershipPatch.setEntityType("dataset");
    ownershipPatch.setEntityUrn(datasetUrn);
    ownershipPatch.setAspectName("ownership");
    ownershipPatch.setChangeType(ChangeType.PATCH);

    List<MetadataChangeProposal> patches = new ArrayList<>();
    patches.add(tagPatch);
    patches.add(ownershipPatch);

    // Create server config for v1.3.0
    ServerConfig serverConfig = createCoreServerConfig("v1.3.0");

    // Transform
    List<datahub.client.v2.patch.TransformResult> results =
        transformer.transform(patches, serverConfig);

    // Should have both patches
    assertEquals(2, results.size());

    // First patch (tag) should remain PATCH (not transformed)
    assertEquals(ChangeType.PATCH, results.get(0).getMcp().getChangeType());
    assertEquals("globalTags", results.get(0).getMcp().getAspectName());
    assertFalse(results.get(0).hasRetryFunction());

    // Second patch (ownership) should be unchanged
    assertEquals(ChangeType.PATCH, results.get(1).getMcp().getChangeType());
    assertEquals("ownership", results.get(1).getMcp().getAspectName());
    assertFalse(results.get(1).hasRetryFunction());

    // Should not call entity client
    verify(mockEntityClient, never()).getAspect(any(), any());
  }

  private MetadataChangeProposal createTagPatch() throws URISyntaxException {
    Urn datasetUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
    return createTagPatchWithUrn(datasetUrn);
  }

  private MetadataChangeProposal createTagPatchWithUrn(Urn entityUrn) throws URISyntaxException {
    // Use GlobalTagsPatchBuilder to create a realistic patch
    GlobalTagsPatchBuilder builder = new GlobalTagsPatchBuilder().urn(entityUrn);
    builder.addTag(TagUrn.createFromString("urn:li:tag:pii"), null);

    MetadataChangeProposal patch = builder.build();
    return patch;
  }

  private MetadataChangeProposal createOwnershipPatch() throws URISyntaxException {
    Urn datasetUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
    MetadataChangeProposal ownershipPatch = new MetadataChangeProposal();
    ownershipPatch.setEntityType("dataset");
    ownershipPatch.setEntityUrn(datasetUrn);
    ownershipPatch.setAspectName("ownership");
    ownershipPatch.setChangeType(ChangeType.PATCH);
    return ownershipPatch;
  }
}
