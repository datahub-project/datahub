package io.datahubproject.openlineage.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Golden-vector test: the expected URNs below are computed independently from the Python side
 * (datahub.sql_parsing.fingerprint_utils.generate_hash), the actual cross-language parity target.
 * They are hard-coded, not derived from {@link QueryUrnUtils} itself, so a hex/padding bug in
 * {@code sha256Hex} cannot pass silently.
 */
public class QueryUrnUtilsTest {

  @Test
  public void testMatchesPythonGenerateHash() {
    Assert.assertEquals(
        QueryUrnUtils.queryUrnForStatement("SELECT col_a FROM my_db.my_schema.src").toString(),
        "urn:li:query:0c84217f3d43ce8ea5c3b6554a9b455b1b6dd937deac6607100393529b7ac547");
  }

  @Test
  public void testEmptyStringMatchesCanonicalSha256() {
    Assert.assertEquals(
        QueryUrnUtils.queryUrnForStatement("").toString(),
        "urn:li:query:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  }
}
