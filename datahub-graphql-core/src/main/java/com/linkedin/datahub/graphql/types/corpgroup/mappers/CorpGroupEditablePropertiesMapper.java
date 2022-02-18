package com.linkedin.datahub.graphql.types.corpgroup.mappers;

import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.generated.CorpGroupEditableProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class CorpGroupEditablePropertiesMapper implements ModelMapper<com.linkedin.identity.CorpGroupEditableInfo, CorpGroupEditableProperties> {

  public static final CorpGroupEditablePropertiesMapper INSTANCE = new CorpGroupEditablePropertiesMapper();

  public static CorpGroupEditableProperties map(@Nonnull final com.linkedin.identity.CorpGroupEditableInfo corpGroupEditableInfo) {
    return INSTANCE.apply(corpGroupEditableInfo);
  }

  @Override
  public CorpGroupEditableProperties apply(@Nonnull final com.linkedin.identity.CorpGroupEditableInfo corpGroupEditableInfo) {
    final CorpGroupEditableProperties result = new CorpGroupEditableProperties();
    result.setDescription(corpGroupEditableInfo.getDescription(GetMode.DEFAULT));
    result.setSlack(corpGroupEditableInfo.getSlack(GetMode.DEFAULT));
    result.setEmail(corpGroupEditableInfo.getEmail(GetMode.DEFAULT));
    return result;
  }
}