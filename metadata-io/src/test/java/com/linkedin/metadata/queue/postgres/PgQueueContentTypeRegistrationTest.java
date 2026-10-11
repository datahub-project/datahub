package com.linkedin.metadata.queue.postgres;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.Test;

/**
 * Contract test for {@link EbeanPostgresMetadataQueueStore#ensureContentTypeRegistered}: lookup
 * first, then {@code INSERT … ON CONFLICT (mime) DO NOTHING} (not {@code DO UPDATE}, which burns
 * {@code smallint} identity on conflict).
 */
public class PgQueueContentTypeRegistrationTest {

  @Test
  public void ensureContentTypeRegistered_lookupThenOnConflictDoNothing() throws Exception {
    Path store =
        Path.of(
            "src/main/java/com/linkedin/metadata/queue/postgres/EbeanPostgresMetadataQueueStore.java");
    String source = Files.readString(store);
    int methodStart = source.indexOf("private short ensureContentTypeRegistered");
    assertTrue(methodStart >= 0, "ensureContentTypeRegistered method present");
    String methodBody = source.substring(methodStart, methodStart + 1500);

    assertTrue(methodBody.contains("lookupContentTypeId(conn, mime)"));
    assertTrue(
        methodBody.indexOf("lookupContentTypeId") < methodBody.indexOf("INSERT INTO"),
        "lookup before insert");
    assertTrue(
        methodBody.contains("ON CONFLICT (mime) DO NOTHING"),
        "use DO NOTHING so concurrent registration does not abort the transaction");
    assertFalse(
        methodBody.contains("ON CONFLICT DO UPDATE"), "DO UPDATE burns identity on conflict");
  }
}
