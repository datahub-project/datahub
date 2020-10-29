package com.linkedin.metadata.builders.search;

import com.linkedin.common.CorpGroupUrnArray;
import com.linkedin.common.CorpuserUrnArray;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import java.net.URISyntaxException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * Util class that contains helper functions for processing snapshot to generate relevant documents
 */

@Slf4j
public final class BuilderUtils {

  public static final String BROWSE_HIEARCHY_SEPARATOR = "/";

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
   * Given {@link CorpuserUrnArray} return list of corp user names from each of the urns
   *
   * @param corpuserUrns {@link CorpuserUrnArray}
   * @return list of user names extracted from urns
   */
  @Nonnull
  public static StringArray getCorpUsernames(@Nonnull CorpuserUrnArray corpuserUrns) {
    return corpuserUrns.stream().map(urn -> urn.getUsernameEntity()).collect(Collectors.toCollection(StringArray::new));
  }

  /**
   * Given {@link CorpGroupUrnArray} return list of corp group names from each of the urns
   *
   * @param corpgroupUrns {@link CorpGroupUrnArray}
   * @return list of group names extracted from urns
   */
  @Nonnull
  public static StringArray getCorpGroupnames(@Nonnull CorpGroupUrnArray corpgroupUrns) {
    return corpgroupUrns.stream().map(urn -> urn.getGroupNameEntity()).collect(Collectors.toCollection(StringArray::new));
  }

  /**
   * Get normalized browse field by replacing special browse hiearchy seperator's with a replacement
   * @param field browse field
   * @param replacement the value which will replace special browse hiearchy seperator
   * @return normalized browse field
   */
  @Nonnull
  public static String getNormalizedBrowseField(@Nonnull String field, @Nonnull String replacement) {
    return field.replace(BROWSE_HIEARCHY_SEPARATOR, replacement);
  }

  /**
   * Given {@link DatasetUrnArray} return list of dataset names from each of the urns
   *
   * @param datasetUrns {@link DatasetUrnArray}
   * @return list of dataset names extracted from urns
   */
  @Nonnull
  public static StringArray getDatasetNames(@Nonnull DatasetUrnArray datasetUrns) {
    return datasetUrns.stream()
        .map(DatasetUrn::getDatasetNameEntity)
        .collect(Collectors.toCollection(StringArray::new));
  }
}
