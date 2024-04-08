package com.linkedin.datahub.graphql.types.glossary.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.GlossaryTerms;
import com.linkedin.datahub.graphql.types.glossary.GlossaryTermUtils;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class GlossaryTermsMapper {

  public static final GlossaryTermsMapper INSTANCE = new GlossaryTermsMapper();

  public static GlossaryTerms map(
      @Nonnull final QueryContext context,
      @Nonnull final com.linkedin.common.GlossaryTerms glossaryTerms,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, glossaryTerms, entityUrn);
  }

  public GlossaryTerms apply(
      @Nonnull final QueryContext context,
      @Nonnull final com.linkedin.common.GlossaryTerms glossaryTerms,
      @Nonnull final Urn entityUrn) {
    com.linkedin.datahub.graphql.generated.GlossaryTerms result =
        new com.linkedin.datahub.graphql.generated.GlossaryTerms();
    result.setTerms(
        glossaryTerms.getTerms().stream()
            .filter(
                association ->
                    context == null || canView(context.getOperationContext(), association.getUrn()))
            .map(association -> this.mapGlossaryTermAssociation(association, entityUrn))
            .collect(Collectors.toList()));
    return result;
  }

  private com.linkedin.datahub.graphql.generated.GlossaryTermAssociation mapGlossaryTermAssociation(
      @Nonnull final GlossaryTermAssociation input, @Nonnull final Urn entityUrn) {
    final com.linkedin.datahub.graphql.generated.GlossaryTermAssociation result =
        new com.linkedin.datahub.graphql.generated.GlossaryTermAssociation();

    final GlossaryTerm resultGlossaryTerm = new GlossaryTerm();
    resultGlossaryTerm.setType(EntityType.GLOSSARY_TERM);
    resultGlossaryTerm.setUrn(input.getUrn().toString());
    resultGlossaryTerm.setName(
        GlossaryTermUtils.getGlossaryTermName(input.getUrn().getNameEntity()));
    result.setTerm(resultGlossaryTerm);

    if (input.hasActor()) {
      CorpUser actor = new CorpUser();
      actor.setUrn(input.getActor().toString());
      actor.setType(EntityType.CORP_USER);
      result.setActor(actor);
    }
    if (entityUrn != null) {
      result.setAssociatedUrn(entityUrn.toString());
    }

    return result;
  }
}
