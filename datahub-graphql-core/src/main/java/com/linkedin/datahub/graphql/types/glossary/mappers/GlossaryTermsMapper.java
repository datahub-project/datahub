package com.linkedin.datahub.graphql.types.glossary.mappers;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryTerms;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.glossary.GlossaryTermUtils;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class GlossaryTermsMapper implements ModelMapper<com.linkedin.common.GlossaryTerms, GlossaryTerms> {

    public static final GlossaryTermsMapper INSTANCE = new GlossaryTermsMapper();

    public static GlossaryTerms map(@Nonnull final com.linkedin.common.GlossaryTerms glossaryTerms) {
        return INSTANCE.apply(glossaryTerms);
    }

    @Override
    public GlossaryTerms apply(@Nonnull final com.linkedin.common.GlossaryTerms glossaryTerms) {
        com.linkedin.datahub.graphql.generated.GlossaryTerms result = new com.linkedin.datahub.graphql.generated.GlossaryTerms();
        result.setTerms(glossaryTerms.getTerms().stream().map(this::mapGlossaryTermAssociation).collect(Collectors.toList()));
        return result;
    }

    private com.linkedin.datahub.graphql.generated.GlossaryTermAssociation mapGlossaryTermAssociation(@Nonnull final GlossaryTermAssociation input) {
        final com.linkedin.datahub.graphql.generated.GlossaryTermAssociation result = new com.linkedin.datahub.graphql.generated.GlossaryTermAssociation();
        final GlossaryTerm resultGlossaryTerm = new GlossaryTerm();
        resultGlossaryTerm.setType(EntityType.GLOSSARY_TERM);
        resultGlossaryTerm.setUrn(input.getUrn().toString());
        resultGlossaryTerm.setName(GlossaryTermUtils.getGlossaryTermName(input.getUrn().getNameEntity()));
        result.setTerm(resultGlossaryTerm);
        return result;
    }

}