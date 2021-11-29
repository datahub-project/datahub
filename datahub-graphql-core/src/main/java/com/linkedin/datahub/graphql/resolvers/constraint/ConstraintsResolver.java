package com.linkedin.datahub.graphql.resolvers.constraint;

import com.datahub.metadata.authorization.ResourceSpec;

import com.linkedin.common.urn.Urn;
import com.linkedin.constraint.ConstraintInfo;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Constraint;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;


@AllArgsConstructor
public class ConstraintsResolver implements DataFetcher<CompletableFuture<List<Constraint>>> {

    private final Function<DataFetchingEnvironment, String> _urnProvider;
    private final EntityClient _entityClient;
    private static final String CONSTRAINT_INFO_ASPECT_NAME = "constraintInfo";

    private Stream<ConstraintInfo> getConstraintInfoAspectsFromConstraints(ListResult listResult, QueryContext context) {
        return listResult.getEntities()
            .stream()
            .map(result -> {
                try {
                    Optional<ConstraintInfo> constraintInfo =
                        _entityClient.getVersionedAspect(
                            result.toString(),
                            CONSTRAINT_INFO_ASPECT_NAME,
                            0L,
                            context.getActor(),
                            ConstraintInfo.class
                        );
                    if (constraintInfo.isPresent()) {
                        return constraintInfo.get();
                    }
                } catch (RemoteInvocationException e) {
                    e.printStackTrace();
                }
                return null;
            }).filter(constraintInfo -> constraintInfo != null);
    }

    @Override
    public CompletableFuture<List<Constraint>> get(DataFetchingEnvironment environment) {

        final QueryContext context = environment.getContext();
        final String urn = _urnProvider.apply(environment);

        return CompletableFuture.supplyAsync(() -> {
            try {
                ResourceSpec spec = new ResourceSpec(Urn.createFromString(urn).getEntityType(), urn);

                final ListResult constraintList = ConstraintCache.getCachedConstraints(_entityClient, context);

                    Stream<ConstraintInfo> aspects = getConstraintInfoAspectsFromConstraints(constraintList, context);

                    return aspects.filter(
                        aspect -> ConstraintUtils.isEntityFailingConstraint(urn, spec, aspect, _entityClient, context.getActor())
                    ).map(ConstraintUtils::mapConstraintInfoToConstraint).collect(Collectors.toList());
            } catch (Exception e) {
                throw new RuntimeException("Failed to load constraints", e);
            }
        });
    }
}
