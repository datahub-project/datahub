/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import { hasOperationName } from "../../utils";
import { SearchBarV2Helper } from "./searchBarV2Helper";

export class SearchV2Helper {
  searchBarV2 = new SearchBarV2Helper();

  ensureItIsSearchPage() {
    cy.url().should("include", "/search");
  }

  ensureUrlIncludesText(text) {
    cy.url().should("include", text);
  }

  ensureIsOnEntityProfilePage(entityUrn) {
    cy.url().should("include", `dataset/${entityUrn}`);
  }

  ensureHasNoResults() {
    cy.contains("of 0 results");
  }

  ensureHasResults() {
    cy.contains("of 0 results").should("not.exist");
    cy.contains(/of [0-9]+ results/);
  }

  goToSearchPage(queryParams) {
    const urlParams = new URLSearchParams(queryParams);
    const url = `/search?${urlParams.toString()}`;
    cy.visit(url);
  }

  goToHomePage() {
    cy.visit("/");
    this.searchBarV2.waitForSearchBar();
  }

  getSearchBarInterceptor(showSearchBarAutocompleteRedesign, searchBarApi) {
    return (req, res) => {
      if (hasOperationName(req, "appConfig")) {
        res.body.data.appConfig.featureFlags.showSearchBarAutocompleteRedesign =
          showSearchBarAutocompleteRedesign;
        res.body.data.appConfig.searchBarConfig.apiVariant = searchBarApi;
      }
    };
  }

  ensureFilterAppliedOnSearchPage(name, value) {
    cy.clickOptionWithTestId(`filter-dropdown-${name}`);
    cy.getWithTestId(`filter-option-${value}`).should("be.checked");
  }
}
