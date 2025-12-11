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

describe("searchBarV2 - Basic Search", () => {
  const helper = new SearchV2Helper();

  const setupTest = (searchBarApi = "SEARCH_ACROSS_ENTITIES") => {
    applyGraphqlInterceptors([
      getThemeV2Interceptor(true),
      helper.getSearchBarInterceptor(true, searchBarApi),
    ]);

    cy.login();
    cy.skipIntroducePage();
  };

  it("can handle all entities search", () => {
    setupTest();
    helper.goToHomePage();

    helper.searchBarV2.type("*{enter}");

    helper.ensureHasResults();
  });

  it("can handle all entities search with an impossible query and find 0 results", () => {
    setupTest();
    helper.goToHomePage();

    helper.searchBarV2.type("zzzzzzzzzzzzzqqqqqqqqqqqqqzzzzzzqzqzqzqzq{enter}");

    helper.ensureHasNoResults();
  });

  it("should show entity in response", () => {
    setupTest();
    helper.goToHomePage();

    helper.searchBarV2.type(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.ensureTextInViewAllSection(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);
  });
});
