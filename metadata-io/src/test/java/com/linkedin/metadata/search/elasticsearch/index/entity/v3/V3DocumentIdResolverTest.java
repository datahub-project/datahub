package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.digest.DigestUtils;
import org.testng.annotations.Test;

public class V3DocumentIdResolverTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
  private static final EntityDocumentIdHasher HASHER = new Sha256UrnEntityDocumentIdHasher();

  @Test
  public void testHashesRawUrn() {
    String expected = DigestUtils.sha256Hex(DATASET_URN.toString());
    assertEquals(
        V3DocumentIdResolver.resolveExplainDocumentId(
            OperationFingerprint.EMPTY, HASHER, DATASET_URN.toString()),
        expected);
  }

  @Test
  public void testHashesUrlEncodedUrn() {
    String encoded = URLEncoder.encode(DATASET_URN.toString(), StandardCharsets.UTF_8);
    assertTrue(encoded.contains("%3A"));
    assertEquals(
        V3DocumentIdResolver.resolveExplainDocumentId(OperationFingerprint.EMPTY, HASHER, encoded),
        DigestUtils.sha256Hex(DATASET_URN.toString()));
  }

  @Test
  public void testLeavesHashedIdUnchanged() {
    String hashed = DigestUtils.sha256Hex(DATASET_URN.toString());
    assertEquals(
        V3DocumentIdResolver.resolveExplainDocumentId(OperationFingerprint.EMPTY, HASHER, hashed),
        hashed);
  }

  @Test
  public void testReplacementHasherIsUsed() {
    EntityDocumentIdHasher replacement =
        (operation, urn) -> DigestUtils.sha256Hex("extra|" + urn.toString());
    assertEquals(
        V3DocumentIdResolver.resolveExplainDocumentId(
            OperationFingerprint.EMPTY, replacement, DATASET_URN.toString()),
        DigestUtils.sha256Hex("extra|" + DATASET_URN));
  }
}
