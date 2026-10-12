package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;

public class V3DocumentIdSchemeTest {

  private final V3DocumentIdScheme scheme =
      new V3DocumentIdScheme(new Sha256UrnEntityDocumentIdHasher());

  @Test
  public void testUnstampedIndexRequiresRebuild() {
    assertTrue(scheme.requiresDocumentRebuild(null));
    assertTrue(scheme.requiresDocumentRebuild(Map.of("properties", Map.of())));
    assertTrue(
        scheme.requiresDocumentRebuild(
            Map.of("_meta", Map.of("other", "value"), "properties", Map.of())));
  }

  @Test
  public void testStampForTheRunningSchemeNeedsNoRebuild() {
    Map<String, Object> stamped = scheme.stampedMappings(Map.of("properties", Map.of()));

    assertFalse(scheme.requiresDocumentRebuild(stamped));
    assertEquals(V3DocumentIdScheme.stampedOn(stamped), Optional.of("sha256-urn"));
  }

  @Test
  public void testStampFromAnotherSchemeRequiresRebuild() {
    EntityDocumentIdHasher tenantHasher =
        new EntityDocumentIdHasher() {
          @Override
          public String documentId(OperationFingerprint operation, Urn urn) {
            return "tenant|" + urn;
          }

          @Override
          public String scheme() {
            return "sha256-tenant-urn";
          }
        };
    Map<String, Object> stampedByTenantHasher =
        new V3DocumentIdScheme(tenantHasher).stampedMappings(Map.of("properties", Map.of()));

    assertTrue(scheme.requiresDocumentRebuild(stampedByTenantHasher));
    assertFalse(
        new V3DocumentIdScheme(tenantHasher).requiresDocumentRebuild(stampedByTenantHasher));
  }

  @Test
  public void testStampedMappingsKeepOtherMetaAndDoNotMutateInput() {
    Map<String, Object> meta = new HashMap<>(Map.of("owner", "datahub"));
    Map<String, Object> mappings =
        new HashMap<>(
            Map.of("_meta", meta, "properties", Map.of("urn", Map.of("type", "keyword"))));

    Map<String, Object> stamped = scheme.stampedMappings(mappings);

    assertEquals(stamped.get("properties"), mappings.get("properties"));
    assertEquals(stamped.get("_meta"), Map.of("owner", "datahub", "docIdScheme", "sha256-urn"));
    assertEquals(meta, Map.of("owner", "datahub"));
    assertFalse(((Map<?, ?>) mappings.get("_meta")).containsKey("docIdScheme"));
  }

  @Test
  public void testDefaultHasherDeclaresAnExplicitScheme() {
    EntityDocumentIdHasher hasher = new Sha256UrnEntityDocumentIdHasher();

    assertEquals(hasher.scheme(), Sha256UrnEntityDocumentIdHasher.SCHEME);
    assertNotEquals(hasher.scheme(), hasher.getClass().getName());
  }
}
