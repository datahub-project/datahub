package datahub.client.v2.entity;

import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;

/**
 * Helper for structured property URN conversion. Package-private for use by HasStructuredProperties
 * (avoids private interface methods for Java 8 compatibility).
 */
final class StructuredPropertyUrns {

  private StructuredPropertyUrns() {}

  @Nonnull
  static Urn makeStructuredPropertyUrn(@Nonnull String propertyUrn) {
    String fullUrn =
        propertyUrn.startsWith("urn:li:structuredProperty:")
            ? propertyUrn
            : "urn:li:structuredProperty:" + propertyUrn;
    try {
      return Urn.createFromString(fullUrn);
    } catch (URISyntaxException e) {
      throw new datahub.client.v2.exceptions.InvalidUrnException(fullUrn, e);
    }
  }
}
