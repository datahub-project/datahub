package com.linkedin.metadata.builders.search;

import com.linkedin.common.MultiLocaleString;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * Util class that contains helper functions for processing snapshot to generate relevant documents
 */

@Slf4j
public final class BuilderUtils {

  private BuilderUtils() {
    // Util class should not have public constructor
  }

  /**
   * Get all corp users' ldap IDs
   *
   * @param ownership {@link Ownership} aspect obtained from a metadata snapshot type
   * @return StringArray containing list of ldap IDs of corp users
   */
  @Nonnull
  public static StringArray getCorpUserOwners(@Nonnull Ownership ownership) {
    final StringArray ldap = new StringArray();
    for (Owner owner : ownership.getOwners()) {
      Urn urn = owner.getOwner();
      if (CorpuserUrn.ENTITY_TYPE.equals(urn.getEntityType())) {
        try {
          ldap.add(CorpuserUrn.createFromUrn(urn).getUsernameEntity());
        } catch (URISyntaxException e) {
          log.error("CorpuserUrn syntax error", e);
        }
      }
    }

    return ldap;
  }

  /**
   * Convert a Multi locale String to users preferred language
   * @param multiLocaleString
   * @param locale {@link String} User's preferred language such as "en_US"
   * @return Localized String
   */
  @Nonnull
  public static String convertMultiLocaleStringToString(@Nonnull MultiLocaleString multiLocaleString, @Nonnull String locale) {
    return multiLocaleString.getLocalized().get(locale);
  }

}
