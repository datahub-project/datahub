package com.linkedin.datahub.graphql.types.common.mappers;

import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * Guard against the "forgot to register a new entity everywhere" regression class.
 *
 * <p>Every entity type exposed in a search / autocomplete / browse list MUST be hydratable into a
 * typed GraphQL entity by {@link UrnToEntityMapper}. {@code SearchResult.entity} is a non-null
 * field, so if a search hit's URN cannot be mapped to an entity, the whole query fails at runtime
 * with {@code NullValueInNonNullableField} — which is exactly how a missing {@code dataObject} case
 * surfaced as a broken container "Contents" tab.
 *
 * <p>Because the search lists ({@link SearchUtils#SEARCHABLE_ENTITY_TYPES} etc.) and {@link
 * UrnToEntityMapper} are independent hand-maintained enumerations, adding a new entity to one but
 * not the other is silent at compile time. This test makes that omission a loud, named CI failure:
 * add the entity to {@link UrnToEntityMapper} (the usual fix) or, only with justification, to
 * {@link #INTENTIONALLY_NOT_HYDRATED}.
 */
public class UrnToEntityMapperCompletenessTest {

  /**
   * Search-surface entity types intentionally NOT hydrated by {@link UrnToEntityMapper}. Keep this
   * empty unless there is a documented reason — the correct fix for a failure is almost always to
   * add a case to {@link UrnToEntityMapper}, not to suppress it here.
   */
  private static final Set<EntityType> INTENTIONALLY_NOT_HYDRATED = Set.of();

  @Test
  public void testEverySearchSurfaceEntityIsHydratable() throws URISyntaxException {
    final Set<EntityType> searchSurface = new LinkedHashSet<>();
    searchSurface.addAll(SearchUtils.SEARCHABLE_ENTITY_TYPES);
    searchSurface.addAll(SearchUtils.AUTO_COMPLETE_ENTITY_TYPES);
    searchSurface.addAll(SearchUtils.BROWSE_ENTITY_TYPES);

    final List<String> failures = new ArrayList<>();
    for (EntityType entityType : searchSurface) {
      if (INTENTIONALLY_NOT_HYDRATED.contains(entityType)) {
        continue;
      }
      final String entityName = EntityTypeMapper.getName(entityType);
      // UrnToEntityMapper keys only on urn.getEntityType(); the urn body is irrelevant here.
      final Urn urn = Urn.createFromString("urn:li:" + entityName + ":guard-test");
      final Entity hydrated = UrnToEntityMapper.map(null, urn);
      if (hydrated == null) {
        failures.add(entityType + " (" + entityName + "): not hydrated (null)");
      } else if (hydrated.getType() != entityType) {
        failures.add(
            entityType + " (" + entityName + "): hydrated as wrong type " + hydrated.getType());
      }
    }

    assertTrue(
        failures.isEmpty(),
        "These entity types appear in a search/autocomplete/browse list but UrnToEntityMapper does "
            + "not hydrate them — search results for these entities fail at runtime with "
            + "NullValueInNonNullableField. Add a case to UrnToEntityMapper.apply() (or, only with "
            + "justification, to INTENTIONALLY_NOT_HYDRATED). Offending: "
            + failures);
  }
}
