package com.linkedin.datahub.graphql.resolvers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.DataMap;
import com.linkedin.data.element.DataElement;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;

import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import graphql.schema.DataFetchingEnvironment;
import java.lang.reflect.InvocationTargetException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.reflect.ConstructorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResolverUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger _logger = LoggerFactory.getLogger(ResolverUtils.class.getName());

    private ResolverUtils() { }

    @Nonnull
    public static <T> T bindArgument(Object argument, Class<T> clazz) {
        return MAPPER.convertValue(argument, clazz);
    }

    /**
     * Returns the string with the forward slash escaped
     * More details on reserved characters in Elasticsearch can be found at,
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters
     */
    @Nonnull
    public static String escapeForwardSlash(@Nonnull String input) {
        if (input.contains("/")) {
            input = input.replace("/", "\\\\/");
        }
        return input;
    }

    @Nonnull
    public static String getActor(DataFetchingEnvironment environment) {
        return ((QueryContext) environment.getContext()).getActor();
    }

    @Nonnull
    public static Map<String, String> buildFacetFilters(@Nullable List<FacetFilterInput> facetFilterInputs,
                                                        @Nonnull Set<String> validFacetFields) {
        if (facetFilterInputs == null) {
            return Collections.emptyMap();
        }

        final Map<String, String> facetFilters = new HashMap<>();

        facetFilterInputs.forEach(facetFilterInput -> {
            if (!validFacetFields.contains(facetFilterInput.getField())) {
                throw new ValidationException(String.format("Unrecognized facet with name %s provided", facetFilterInput.getField()));
            }
            facetFilters.put(facetFilterInput.getField(), facetFilterInput.getValue());
        });

        return facetFilters;
    }

    @Nullable
    public static Filter buildFilter(@Nullable List<FacetFilterInput> facetFilterInputs) {
        if (facetFilterInputs == null) {
            return null;
        }
        return new Filter().setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(facetFilterInputs.stream()
            .map(filter -> new Criterion().setField(filter.getField()).setValue(filter.getValue()))
            .collect(Collectors.toList())))));
    }

    private static Object constructAspectFromDataElement(DataElement aspectDataElement)
        throws ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException {
        String restliAspectClassName = aspectDataElement.getSchema().getUnionMemberKey();
        // construct the restli aspect class from the aspect's DataMap stored in local context
        Object constructedAspect = Class.forName(restliAspectClassName)
            .cast((
                ConstructorUtils.getMatchingAccessibleConstructor(
                    Class.forName(restliAspectClassName),
                    new Class[]{DataMap.class}
                ).newInstance(aspectDataElement.getValue())
            ));

        return constructedAspect;
    }

    private static com.linkedin.metadata.aspect.Aspect constructAspectUnionInstanceFromAspect(Object constructedAspect)
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        return (com.linkedin.metadata.aspect.Aspect)
            com.linkedin.metadata.aspect.Aspect.class.getMethod("create", constructedAspect.getClass())
                .invoke(com.linkedin.metadata.aspect.Aspect.class, constructedAspect);
    }

    @Nonnull
    public static VersionedAspect getAspectFromLocalContext(DataFetchingEnvironment environment) {
        String fieldName = environment.getField().getName();
        Long version = environment.getArgument("version");

        Object localContext = environment.getLocalContext();
        // if we have context & the version is 0, we should try to retrieve it from the fetched entity
        // otherwise, we should just fetch the entity from the aspect resource
        if (localContext != null && version == 0 || version == null) {
            if (localContext instanceof Map) {
                // de-register the prefetched aspect from local context. Since aspects will only
                // ever be first-class properties of an entity type, local context will always
                // contain a map of { aspectName: DataMap }
                DataElement prefetchedAspect =
                    ((Map<String, DataElement>) localContext).getOrDefault(fieldName, null);

                if (prefetchedAspect != null) {
                    try {
                        Object constructedAspect = constructAspectFromDataElement(prefetchedAspect);

                        VersionedAspect resultWithMetadata = new VersionedAspect();

                        resultWithMetadata.setAspect(constructAspectUnionInstanceFromAspect(constructedAspect));

                        resultWithMetadata.setVersion(0);

                        return resultWithMetadata;
                    } catch (IllegalAccessException | InstantiationException | InvocationTargetException | ClassNotFoundException | NoSuchMethodException e) {
                        _logger.error(
                            "Error fetch aspect from local context. field: {} version: {}. Error: {}",
                            fieldName,
                            version,
                            e.toString()
                        );
                        e.printStackTrace();
                    }
                }
            }
        }
        return null;
    }
}
