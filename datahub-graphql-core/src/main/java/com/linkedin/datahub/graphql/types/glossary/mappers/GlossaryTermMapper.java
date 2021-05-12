package com.linkedin.datahub.graphql.types.glossary.mappers;

import javax.annotation.Nonnull;

import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class GlossaryTermMapper implements ModelMapper<com.linkedin.glossary.GlossaryTerm, GlossaryTerm> {

    public static final GlossaryTermMapper INSTANCE = new GlossaryTermMapper();

    public static GlossaryTerm map(@Nonnull final com.linkedin.glossary.GlossaryTerm glossaryTerm) {
        return INSTANCE.apply(glossaryTerm);
    }

    @Override
    public GlossaryTerm apply(@Nonnull final com.linkedin.glossary.GlossaryTerm glossaryTerm) {
        com.linkedin.datahub.graphql.generated.GlossaryTerm result = new com.linkedin.datahub.graphql.generated.GlossaryTerm();
        result.setUrn(glossaryTerm.getUrn().toString());
        result.setType(EntityType.GLOSSARY_TERM);
        result.setName(glossaryTerm.getUrn().getNameEntity());
        result.setGlossaryTermInfo(GlossaryTermInfoMapper.map(glossaryTerm.getGlossaryTermInfo()));
        return result;
    }
}