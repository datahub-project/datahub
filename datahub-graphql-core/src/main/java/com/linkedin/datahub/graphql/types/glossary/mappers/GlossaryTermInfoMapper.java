package com.linkedin.datahub.graphql.types.glossary.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.GlossaryTermInfo;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class GlossaryTermInfoMapper {

  public static final GlossaryTermInfoMapper INSTANCE = new GlossaryTermInfoMapper();

  public static GlossaryTermInfo map(
      @Nonnull final com.linkedin.glossary.GlossaryTermInfo glossaryTermInfo, Urn entityUrn) {
    return INSTANCE.apply(glossaryTermInfo, entityUrn);
  }

  public GlossaryTermInfo apply(
      @Nonnull final com.linkedin.glossary.GlossaryTermInfo glossaryTermInfo, Urn entityUrn) {
    com.linkedin.datahub.graphql.generated.GlossaryTermInfo glossaryTermInfoResult =
        new com.linkedin.datahub.graphql.generated.GlossaryTermInfo();
    glossaryTermInfoResult.setDefinition(glossaryTermInfo.getDefinition());
    glossaryTermInfoResult.setDescription(glossaryTermInfo.getDefinition());
    glossaryTermInfoResult.setTermSource(glossaryTermInfo.getTermSource());
    if (glossaryTermInfo.hasName()) {
      glossaryTermInfoResult.setName(glossaryTermInfo.getName());
    }
    if (glossaryTermInfo.hasSourceRef()) {
      glossaryTermInfoResult.setSourceRef(glossaryTermInfo.getSourceRef());
    }
    if (glossaryTermInfo.hasSourceUrl()) {
      glossaryTermInfoResult.setSourceUrl(glossaryTermInfo.getSourceUrl().toString());
    }
    if (glossaryTermInfo.hasCustomProperties()) {
      glossaryTermInfoResult.setCustomProperties(
          CustomPropertiesMapper.map(glossaryTermInfo.getCustomProperties(), entityUrn));
    }
    return glossaryTermInfoResult;
  }
}
