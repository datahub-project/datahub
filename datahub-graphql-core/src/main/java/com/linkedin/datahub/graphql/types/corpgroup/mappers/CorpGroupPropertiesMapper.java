package com.linkedin.datahub.graphql.types.corpgroup.mappers;

import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.generated.CorpGroupProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class CorpGroupPropertiesMapper implements ModelMapper<com.linkedin.identity.CorpGroupInfo, CorpGroupProperties> {

  public static final CorpGroupPropertiesMapper INSTANCE = new CorpGroupPropertiesMapper();

  public static CorpGroupProperties map(@Nonnull final com.linkedin.identity.CorpGroupInfo corpGroupInfo) {
    return INSTANCE.apply(corpGroupInfo);
  }

  @Override
  public CorpGroupProperties apply(@Nonnull final com.linkedin.identity.CorpGroupInfo info) {
    final CorpGroupProperties result = new CorpGroupProperties();
    result.setEmail(info.getEmail());
    result.setDescription(info.getDescription());
    result.setDisplayName(info.getDisplayName());
    result.setSlack(info.getSlack(GetMode.DEFAULT));
    return result;
  }
}
