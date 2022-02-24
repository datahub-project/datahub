package com.linkedin.datahub.graphql.types.glossary.mappers;

import javax.annotation.Nonnull;

import com.linkedin.datahub.graphql.generated.GlossaryTermInfo;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class GlossaryTermInfoMapper implements ModelMapper<com.linkedin.glossary.GlossaryTermInfo, GlossaryTermInfo> {

    public static final GlossaryTermInfoMapper INSTANCE = new GlossaryTermInfoMapper();

    public static GlossaryTermInfo map(@Nonnull final com.linkedin.glossary.GlossaryTermInfo glossaryTermInfo) {
        return INSTANCE.apply(glossaryTermInfo);
    }

    @Override
    public GlossaryTermInfo apply(@Nonnull final com.linkedin.glossary.GlossaryTermInfo glossaryTermInfo) {
        com.linkedin.datahub.graphql.generated.GlossaryTermInfo glossaryTermInfoResult = new com.linkedin.datahub.graphql.generated.GlossaryTermInfo();
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
            glossaryTermInfoResult.setCustomProperties(StringMapMapper.map(glossaryTermInfo.getCustomProperties()));
        }       
        return glossaryTermInfoResult;
    }
}
