package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.apache.commons.codec.digest.DigestUtils;
import org.testng.annotations.Test;

public class Sha256UrnEntityDocumentIdHasherTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");

  @Test
  public void testHashesUrnAndIgnoresFingerprint() {
    Sha256UrnEntityDocumentIdHasher hasher = new Sha256UrnEntityDocumentIdHasher();
    String expected = DigestUtils.sha256Hex(DATASET_URN.toString());

    String emptyId = hasher.documentId(OperationFingerprint.EMPTY, DATASET_URN);
    String populatedId =
        hasher.documentId(TestOperationContexts.systemContextNoSearchAuthorization(), DATASET_URN);

    assertEquals(emptyId, expected);
    assertEquals(populatedId, expected);
    assertFalse(emptyId.contains("urn:li:"));
  }

  @Test
  public void testDifferentUrnsProduceDifferentIds() {
    Sha256UrnEntityDocumentIdHasher hasher = new Sha256UrnEntityDocumentIdHasher();
    Urn other = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,OtherHdfsDataset,PROD)");
    assertNotEquals(
        hasher.documentId(OperationFingerprint.EMPTY, DATASET_URN),
        hasher.documentId(OperationFingerprint.EMPTY, other));
  }

  @Test
  public void testReplacementHasherChangesId() {
    EntityDocumentIdHasher replacement =
        (operation, urn) -> DigestUtils.sha256Hex("extra|" + urn.toString());
    Sha256UrnEntityDocumentIdHasher ossDefault = new Sha256UrnEntityDocumentIdHasher();
    assertNotEquals(
        replacement.documentId(OperationFingerprint.EMPTY, DATASET_URN),
        ossDefault.documentId(OperationFingerprint.EMPTY, DATASET_URN));
  }
}
