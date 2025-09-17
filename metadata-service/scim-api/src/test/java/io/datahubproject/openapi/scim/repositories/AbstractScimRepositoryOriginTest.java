package io.datahubproject.openapi.scim.repositories;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.common.Origin;
import com.linkedin.common.OriginType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.key.CorpUserKey;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for AbstractScimRepository getExternalId() method. These tests focus specifically on
 * the getExternalId() logic.
 */
public class AbstractScimRepositoryOriginTest {

  // Minimal test repository just to access ScimReadableCorpEntity
  private static class TestRepository extends ScimUserRepository {
    // Just need access to the inner class
  }

  @Test
  public void testGetExternalIdWithNativeOrigin() {
    TestRepository repo = new TestRepository();

    // Create Origin with NATIVE type
    Origin nativeOrigin = new Origin();
    nativeOrigin.setType(OriginType.NATIVE);

    Map<Class<? extends RecordTemplate>, RecordTemplate> aspects = new HashMap<>();
    aspects.put(Origin.class, nativeOrigin);

    // Create entity and test getExternalId via asScimResource
    CorpuserUrn urn = new CorpuserUrn("testuser");
    CorpUserKey key = new CorpUserKey();
    key.setUsername("testuser");

    AbstractScimRepository.ScimReadableCorpEntity entity =
        repo
        .new ScimReadableCorpEntity(
            urn, key, System.currentTimeMillis(), System.currentTimeMillis(), aspects);

    // Test: getExternalId should return null for NATIVE origin
    String externalId = entity.getExternalId();
    assertNull(externalId, "External ID should be null for NATIVE origin type");
  }

  @Test
  public void testGetExternalIdWithNullOrigin() {
    TestRepository repo = new TestRepository();

    // No origin aspect
    Map<Class<? extends RecordTemplate>, RecordTemplate> aspects = new HashMap<>();

    CorpuserUrn urn = new CorpuserUrn("testuser");
    CorpUserKey key = new CorpUserKey();
    key.setUsername("testuser");

    AbstractScimRepository.ScimReadableCorpEntity entity =
        repo
        .new ScimReadableCorpEntity(
            urn, key, System.currentTimeMillis(), System.currentTimeMillis(), aspects);

    // Test: getExternalId should return null when no origin aspect exists
    String externalId = entity.getExternalId();
    assertNull(externalId, "External ID should be null when origin aspect is missing");
  }

  @Test
  public void testGetExternalIdWithOriginNoType() {
    TestRepository repo = new TestRepository();

    // Create Origin without type set
    Origin origin = new Origin();
    // Don't set type - hasType() will return false

    Map<Class<? extends RecordTemplate>, RecordTemplate> aspects = new HashMap<>();
    aspects.put(Origin.class, origin);

    CorpuserUrn urn = new CorpuserUrn("testuser");
    CorpUserKey key = new CorpUserKey();
    key.setUsername("testuser");

    AbstractScimRepository.ScimReadableCorpEntity entity =
        repo
        .new ScimReadableCorpEntity(
            urn, key, System.currentTimeMillis(), System.currentTimeMillis(), aspects);

    // Test: getExternalId should return null when origin has no type
    String externalId = entity.getExternalId();
    assertNull(externalId, "External ID should be null when origin has no type");
  }

  @Test
  public void testGetExternalIdWithExternalOriginNullExternalType() {
    TestRepository repo = new TestRepository();

    // Create Origin with EXTERNAL type but null externalType
    Origin externalOrigin = new Origin();
    externalOrigin.setType(OriginType.EXTERNAL);
    // Don't set externalType - it will be null

    Map<Class<? extends RecordTemplate>, RecordTemplate> aspects = new HashMap<>();
    aspects.put(Origin.class, externalOrigin);

    CorpuserUrn urn = new CorpuserUrn("testuser");
    CorpUserKey key = new CorpUserKey();
    key.setUsername("testuser");

    AbstractScimRepository.ScimReadableCorpEntity entity =
        repo
        .new ScimReadableCorpEntity(
            urn, key, System.currentTimeMillis(), System.currentTimeMillis(), aspects);

    // Test: getExternalId should return null for EXTERNAL origin with null externalType (line 666)
    String externalId = entity.getExternalId();
    assertNull(externalId, "External ID should be null for EXTERNAL origin with null externalType");
  }

  @Test
  public void testGetExternalIdWithExternalOriginValidExternalType() {
    TestRepository repo = new TestRepository();

    // Create Origin with EXTERNAL type and valid externalType
    String expectedExternalId = "test123";
    Origin externalOrigin = new Origin();
    externalOrigin.setType(OriginType.EXTERNAL);
    externalOrigin.setExternalType("SCIM_client_" + expectedExternalId);

    Map<Class<? extends RecordTemplate>, RecordTemplate> aspects = new HashMap<>();
    aspects.put(Origin.class, externalOrigin);

    CorpuserUrn urn = new CorpuserUrn("testuser");
    CorpUserKey key = new CorpUserKey();
    key.setUsername("testuser");

    AbstractScimRepository.ScimReadableCorpEntity entity =
        repo
        .new ScimReadableCorpEntity(
            urn, key, System.currentTimeMillis(), System.currentTimeMillis(), aspects);

    // Test: getExternalId should return the external ID
    String externalId = entity.getExternalId();
    assertEquals(expectedExternalId, externalId, "External ID should match the expected value");
  }

  @Test
  public void testGetExternalIdWithExternalOriginNullString() {
    TestRepository repo = new TestRepository();

    // Create Origin with EXTERNAL type and "null" string as externalType
    Origin externalOrigin = new Origin();
    externalOrigin.setType(OriginType.EXTERNAL);
    externalOrigin.setExternalType("SCIM_client_null");

    Map<Class<? extends RecordTemplate>, RecordTemplate> aspects = new HashMap<>();
    aspects.put(Origin.class, externalOrigin);

    CorpuserUrn urn = new CorpuserUrn("testuser");
    CorpUserKey key = new CorpUserKey();
    key.setUsername("testuser");

    AbstractScimRepository.ScimReadableCorpEntity entity =
        repo
        .new ScimReadableCorpEntity(
            urn, key, System.currentTimeMillis(), System.currentTimeMillis(), aspects);

    // Test: getExternalId should return null when externalType contains 'null' string
    String externalId = entity.getExternalId();
    assertNull(externalId, "External ID should be null when externalType contains 'null' string");
  }
}
