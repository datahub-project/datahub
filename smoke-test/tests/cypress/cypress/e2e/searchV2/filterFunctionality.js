/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import { applyGraphqlInterceptors, getThemeV2Interceptor } from "../utils";
import { SearchV2Helper } from "./helpers/searchV2Helper";

const SAMPLE_ENTITY_NAME = "SampleCypressKafkaDataset";
const SAMPLE_ENTITY_URN =
  "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)";

describe("searchBarV2 - Filter Functionality", () => {
  const helper = new SearchV2Helper();

  const setupTest = (searchBarApi = "SEARCH_ACROSS_ENTITIES") => {
    applyGraphqlInterceptors([
      getThemeV2Interceptor(true),
      helper.getSearchBarInterceptor(true, searchBarApi),
    ]);

    cy.login();
    cy.skipIntroducePage();
  };

  it("should show no results state", () => {
    setupTest();
    helper.goToHomePage();

    helper.searchBarV2.type(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.filters.apply("platform", ["Looker"]);
    helper.searchBarV2.filters.ensureValuesSelected("platform", ["Looker"]);
    helper.searchBarV2.ensureInNoResultsFoundState();
    helper.searchBarV2.ensureNoResultsFoundStateHasClearButton();
    helper.searchBarV2.clearFiltersInNoResultsFoundState();
    // filters cleared only
    helper.searchBarV2.filters.ensureValuesNotSelected("platform", ["Looker"]);
    helper.searchBarV2.ensureTextInSearchBar(SAMPLE_ENTITY_NAME);

    helper.searchBarV2.clear();
    helper.searchBarV2.type(`${SAMPLE_ENTITY_NAME}zzzzzzzzzzzzzzzz`);
    helper.searchBarV2.ensureInNoResultsFoundState();
    // without filters there are no clear button
    helper.searchBarV2.ensureNoResultsFoundStateHasNoClearButton();
  });
});
