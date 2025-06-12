import { hasOperationName } from "../../utils";
import { SearchBarV2Helper } from "./searchBarV2Helper";

export class SearchV2Helper {
  searchBarV2 = new SearchBarV2Helper();

  ensureItIsSeachPage() {
    cy.url().should("include", "/search");
  }

  ensureUrlIncludesText(text) {
    cy.url().should("include", text);
  }

  ensureIsOnEntityProfilePage(entityUrn) {
    cy.url().should("include", `dataset/${entityUrn}`);
  }

  waitForSearchPageLoading() {
    cy.wait(5000);
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

    this.waitForSearchPageLoading();
  }

  goToHomePage() {
    cy.visit("/");
    cy.wait(1000);
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
}
