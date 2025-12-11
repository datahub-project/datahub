/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.search.query.request;

import com.linkedin.metadata.models.SearchableRefFieldSpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.testng.annotations.Test;

@Test
public class TestSearchFieldConfig {

  void setup() {}

  /**
   *
   *
   * <ul>
   *   <li>{@link SearchFieldConfig#detectSubFieldType( SearchableRefFieldSpec, int, EntityRegistry
   *       ) }
   * </ul>
   */
  @Test
  public void detectSubFieldType() {
    EntityRegistry entityRegistry = getTestEntityRegistry();
    SearchableRefFieldSpec searchableRefFieldSpec =
        entityRegistry.getEntitySpec("testRefEntity").getSearchableRefFieldSpecs().get(0);

    Set<SearchFieldConfig> responseForNonZeroDepth =
        SearchFieldConfig.detectSubFieldType(searchableRefFieldSpec, 1, entityRegistry);
    Assertions.assertTrue(
        responseForNonZeroDepth.stream()
            .anyMatch(
                searchFieldConfig ->
                    searchFieldConfig.fieldName().equals("refEntityUrns.displayName")));
    Assertions.assertTrue(
        responseForNonZeroDepth.stream()
            .anyMatch(
                searchFieldConfig -> searchFieldConfig.fieldName().equals("refEntityUrns.urn")));
    Assertions.assertTrue(
        responseForNonZeroDepth.stream()
            .anyMatch(
                searchFieldConfig ->
                    searchFieldConfig.fieldName().equals("refEntityUrns.editedFieldDescriptions")));

    Set<SearchFieldConfig> responseForZeroDepth =
        SearchFieldConfig.detectSubFieldType(searchableRefFieldSpec, 0, entityRegistry);
    Optional<SearchFieldConfig> searchFieldConfigToCompare =
        responseForZeroDepth.stream()
            .filter(searchFieldConfig -> searchFieldConfig.fieldName().equals("refEntityUrns"))
            .findFirst();

    Assertions.assertTrue(searchFieldConfigToCompare.isPresent());
    Assertions.assertEquals("query_urn_component", searchFieldConfigToCompare.get().analyzer());
  }

  private EntityRegistry getTestEntityRegistry() {
    return new ConfigEntityRegistry(
        TestSearchFieldConfig.class
            .getClassLoader()
            .getResourceAsStream("test-entity-registry.yaml"));
  }
}
