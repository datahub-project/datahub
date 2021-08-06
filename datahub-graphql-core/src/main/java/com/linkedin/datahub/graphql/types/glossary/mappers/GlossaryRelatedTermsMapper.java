package com.linkedin.datahub.graphql.types.glossary.mappers;
import javax.annotation.Nonnull;
import com.linkedin.datahub.graphql.generated.GlossaryRelatedTerms;
import com.linkedin.datahub.graphql.types.glossary.GlossaryTermUtils;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;

public class GlossaryRelatedTermsMapper implements ModelMapper<com.linkedin.glossary.GlossaryRelatedTerms, GlossaryRelatedTerms> {
    public static final GlossaryRelatedTermsMapper INSTANCE = new GlossaryRelatedTermsMapper();

    public static GlossaryRelatedTerms map(@Nonnull final com.linkedin.glossary.GlossaryRelatedTerms glossaryRelatedTerms) {
        return INSTANCE.apply(glossaryRelatedTerms);
    }

    @Override
    public GlossaryRelatedTerms apply(@Nonnull final com.linkedin.glossary.GlossaryRelatedTerms glossaryRelatedTerms) {
        GlossaryRelatedTerms glossaryRelatedTermsResult = new GlossaryRelatedTerms();
        if(glossaryRelatedTerms.hasHasRelatedTerms()){
            glossaryRelatedTermsResult.setHasRelatedTerms(glossaryRelatedTerms.getHasRelatedTerms().stream().map(urn -> {
                final GlossaryTerm glossaryTerm = new GlossaryTerm();
                glossaryTerm.setUrn(urn.toString());
                glossaryTerm.setName(GlossaryTermUtils.getGlossaryTermName(urn.getNameEntity()));
                return glossaryTerm;
            }).collect(Collectors.toList()));
        }
        if(glossaryRelatedTerms.hasIsRelatedTerms()){
            glossaryRelatedTermsResult.setIsRelatedTerms(glossaryRelatedTerms.getIsRelatedTerms().stream().map(urn -> {
                final GlossaryTerm glossaryTerm = new GlossaryTerm();
                glossaryTerm.setUrn(urn.toString());
                glossaryTerm.setName(GlossaryTermUtils.getGlossaryTermName(urn.getNameEntity()));
                return glossaryTerm;
            }).collect(Collectors.toList()));
        }
        return glossaryRelatedTermsResult;
    }

}
