package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.mapInputFlags;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.datahub.graphql.generated.FormForActor;
import com.linkedin.datahub.graphql.generated.GetFormsForActorInput;
import com.linkedin.datahub.graphql.generated.GetFormsForActorResult;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GetFormsForActorResolver
    implements DataFetcher<CompletableFuture<GetFormsForActorResult>> {

  private final GroupService _groupService;
  private final FormService _formService;

  public GetFormsForActorResolver(
      @Nonnull final GroupService groupService, @Nonnull final FormService formService) {
    _groupService = Objects.requireNonNull(groupService, "groupService must not be null");
    _formService = Objects.requireNonNull(formService, "formService must not be null");
  }

  @Override
  public CompletableFuture<GetFormsForActorResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();

    final Urn userUrn = UrnUtils.getUrn(context.getActorUrn());
    GetFormsForActorInput input = null;
    if (environment.getArgument("input") != null) {
      input = bindArgument(environment.getArgument("input"), GetFormsForActorInput.class);
    }
    final SearchFlags searchFlags =
        input != null ? mapInputFlags(context, input.getSearchFlags()) : null;
    final OperationContext opContext =
        context.getOperationContext().withSearchFlags(flags -> searchFlags);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // get forms explicitly assigned to me on the form itself
            final List<Urn> groupUrns = _groupService.getGroupsForUser(opContext, userUrn);
            final List<Urn> formsExplicitlyAssigned =
                _formService.getFormsAssignedToActor(opContext, userUrn, groupUrns);

            // get forms implicitly assigned to me because I own an entity with an ownership form
            final List<Urn> ownershipForms = _formService.getOwnershipForms(opContext);
            final List<Urn> formsByOwnership =
                _formService.getFormsAssignedByOwnership(
                    opContext,
                    SearchUtils.getEntityNames(null),
                    userUrn,
                    groupUrns,
                    ownershipForms);

            final Set<Urn> allFormUrns = new HashSet<>();
            allFormUrns.addAll(formsExplicitlyAssigned);
            allFormUrns.addAll(formsByOwnership);

            GetFormsForActorResult result = new GetFormsForActorResult();
            List<FormForActor> formsForActor =
                allFormUrns.stream().map(this::mapFormForActor).collect(Collectors.toList());
            result.setFormsForActor(formsForActor);

            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to get forms for the given user.", e);
          }
        });
  }

  private FormForActor mapFormForActor(@Nonnull final Urn formUrn) {
    FormForActor formForActor = new FormForActor();
    Form form = new Form();
    form.setUrn(formUrn.toString());
    form.setType(EntityType.FORM);
    formForActor.setForm(form);
    return formForActor;
  }
}
