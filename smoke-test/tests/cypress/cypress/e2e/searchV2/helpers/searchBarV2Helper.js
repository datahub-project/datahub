import { SearchBarV2FiltersHelper } from "./searchBarV2FiltersHelper";

export class SearchBarV2Helper {
  filters = new SearchBarV2FiltersHelper();

  searchBarTestId = "search-bar";
  searchBarDropdownTestId = "search-bar-dropdown";
  autocompleteItemTestIdPrefix = "autocomplete-item";
  viewAllSectionTestId = "view-all-results";
  filterBaseButtonClearTestId = "button-clear";
  filterSearchInputTestId = "dropdown-search-bar";
  filterUpdateButtonTestId = "footer-button-update";
  filterCancellButtonTestId = "footer-button-cancel";
  noResultsFoundTestId = "no-results-found";
  noResultsFoundClearButtonTestId = "no-results-found-button-clear";

  click() {
    cy.clickOptionWithTestId(this.searchBarTestId);
  }

  get() {
    return cy.getWithTestId(this.searchBarTestId);
  }

  openByShortcut() {
    cy.get("body").type("{ctrl}{shift}k", { release: true });
    cy.focused().clear();
  }

  focus() {
    this.get().within(() => cy.get("input").focus());
  }

  type(text) {
    cy.enterTextInTestId(this.searchBarTestId, text);
  }

  clear() {
    this.get().clear();
  }

  pressEscape() {
    cy.get("body").type("{esc}", { release: true });
  }

  ensureFocused() {
    this.get().within(() => cy.get("input").focused());
  }

  getDropdown() {
    return cy.getWithTestId(this.searchBarDropdownTestId);
  }

  ensureOpened() {
    this.getDropdown().should("be.visible");
  }

  ensureClosed() {
    this.getDropdown().should("not.be.visible");
  }

  clickOnEntityResponseItem(entityUrn) {
    cy.clickOptionWithTestId(this.getTestIdForEntityResponseItem(entityUrn));
  }

  ensureTextInSearchBar(text) {
    this.get().get("input").should("have.value", text);
  }

  getEntitiResponseItem(entityUrn) {
    return cy.getWithTestId(this.getTestIdForEntityResponseItem(entityUrn));
  }

  ensureEntityInResponse(entityUrn) {
    this.getEntitiResponseItem(entityUrn).should("be.visible");
  }

  ensureEntityNotInResponse(entityUrn) {
    this.getEntitiResponseItem(entityUrn).should("not.exist");
  }

  ensureTextInViewAllSection(text) {
    cy.getWithTestId(this.viewAllSectionTestId)
      .should("be.visible")
      .contains(text);
  }

  clickOnViewAllSection() {
    cy.clickOptionWithTestId(this.viewAllSectionTestId);
  }

  ensureInNoResultsFoundState() {
    cy.getWithTestId(this.noResultsFoundTestId).should("be.visible");
  }

  clearFiltersInNoResultsFoundState() {
    cy.clickOptionWithTestId(this.noResultsFoundClearButtonTestId);
    this.waitForApiResponse(); // wait for search bar's response after clearing filters
  }

  ensureNoResultsFoundStateHasClearButton() {
    cy.getWithTestId(this.noResultsFoundClearButtonTestId).should("be.visible");
  }

  ensureNoResultsFoundStateHasNoClearButton() {
    cy.getWithTestId(this.noResultsFoundClearButtonTestId).should("not.exist");
  }

  ensureMatchedFieldHasText(entityUrn, field, value) {
    this.getEntitiResponseItem(entityUrn).within(() => {
      cy.getWithTestId(`matched-field-container-${field}`)
        .first()
        .within(() => {
          cy.getWithTestId("matched-field-value")
            .invoke("text")
            .should("include", value);
        });
    });
  }

  getTestIdForEntityResponseItem(entityUrn) {
    return `${this.autocompleteItemTestIdPrefix}-${entityUrn}`;
  }

  waitForApiResponse() {
    cy.wait(2000);
  }
}
