package graphql.resolvers.query;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.dao.DaoFactory;
import com.linkedin.datahub.dao.view.BrowseDAO;
import com.linkedin.datahub.dao.view.DocumentSearchDao;
import graphql.resolvers.exception.ValueValidationError;

import javax.annotation.Nullable;
import java.util.*;

import static utils.SearchUtil.CORP_USER_FACET_FIELDS;
import static utils.SearchUtil.DATASET_FACET_FIELDS;

public class ResolverUtil {

    private static final String USER_TYPE = "USER";
    private static final String DATASET_TYPE = "DATASET";

    public static final ImmutableMap<String, Set<String>> FACET_FIELDS = ImmutableMap.of(
            USER_TYPE, CORP_USER_FACET_FIELDS,
            DATASET_TYPE, DATASET_FACET_FIELDS
    );

    static Map<String, DocumentSearchDao> SEARCH_DAO_MAP = ImmutableMap.<String, DocumentSearchDao>builder()
        .put(USER_TYPE,DaoFactory.getCorpUserDocumentSearchDao())
        .put(DATASET_TYPE, DaoFactory.getDatasetDocumentSearchDao())
        .build();

    static Map<String, BrowseDAO> BROWSE_DAO_MAP = ImmutableMap.<String, BrowseDAO>builder()
        .put(DATASET_TYPE, DaoFactory.getDatasetBrowseDAO())
        .build();

    static Map<String, String> buildFacetFilters(@Nullable List<Map<String, String>> facetFiltersInput, String entityType) {
        if (facetFiltersInput == null) {
            return Collections.emptyMap();
        }

        final Set<String> validFacets = FACET_FIELDS.getOrDefault(entityType, Collections.emptySet());
        final Map<String, String> facetFilters = new HashMap<>();

        facetFiltersInput.forEach(facetFilterInput -> {
            if (!validFacets.contains(facetFilterInput.get("field"))) {
                throw new ValueValidationError(String.format("Unrecognized facet with name %s provided", facetFilterInput.get("field")));
            }
            facetFilters.put(facetFilterInput.get("field"), facetFilterInput.get("value"));
        });

        return facetFilters;
    }

}
