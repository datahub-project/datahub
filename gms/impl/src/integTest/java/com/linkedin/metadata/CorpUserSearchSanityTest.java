package com.linkedin.metadata;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.search.CorpUserInfoDocument;
import com.linkedin.metadata.testing.BaseSearchSanityTests;
import com.linkedin.metadata.testing.SearchIndex;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import javax.annotation.Nonnull;


public class CorpUserSearchSanityTest extends BaseSearchSanityTests<CorpUserInfoDocument> {
  @SearchIndexType(CorpUserInfoDocument.class)
  @SearchIndexSettings("/index/corp-user/settings.json")
  @SearchIndexMappings("/index/corp-user/mappings.json")
  public SearchIndex<CorpUserInfoDocument> _index;

  private static final String TEST_USERNAME = "fbaggins";
  private static final CorpuserUrn URN = new CorpuserUrn(TEST_USERNAME);
  private static final CorpUserInfoDocument DOCUMENT = new CorpUserInfoDocument().setUrn(URN)
      .setAboutMe("Simply walking into Mordor.")
      .setActive(true)
      .setEmails(new StringArray("frodo@linkedin.com", "baggins@linkedin.com"))
      .setFullName("Frodo Baggins")
      .setLdap(TEST_USERNAME)
      .setManagerLdap("gwhite")
      .setRemoved(false)
      .setSkills(new StringArray("ring bearer", "tricksy"))
      .setTeams(new StringArray("the shire", "the fellowship"))
      .setTitle("ring bearer");

  protected CorpUserSearchSanityTest() {
    super(URN, DOCUMENT);
  }

  @Nonnull
  @Override
  public SearchIndex<CorpUserInfoDocument> getIndex() {
    return _index;
  }
}
