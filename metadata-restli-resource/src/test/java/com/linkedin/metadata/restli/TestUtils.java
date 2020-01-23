package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.testing.Document;
import com.linkedin.testing.Key;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;


public class TestUtils {

  private TestUtils() {
    // util class
  }

  @Nonnull
  public static Urn makeUrn(long id) {
    try {
      return new Urn("urn:li:testing:" + id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static Key makeKey(long id) {
    return new Key().setId(id);
  }

  @Nonnull
  public static ComplexResourceKey<Key, EmptyRecord> makeResourceKey(@Nonnull Urn urn) {
    return new ComplexResourceKey<>(makeKey(urn.getIdAsLong()), new EmptyRecord());
  }

  @Nonnull
  public static Document makeDocument(@Nonnull Urn urn) {
    return new Document().setUrn(urn);
  }

}
