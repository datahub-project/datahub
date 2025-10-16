import { SearchBarV2FiltersHelper } from "./searchBarV2FiltersHelper";

export class SearchBarV2Helper {
  filters = new SearchBarV2FiltersHelper();

  searchBarTestId = "search-bar";

  searchBarWrapperTestId = "v2-search-bar-wrapper";

  searchBarDropdownTestId = "search-bar-dropdown";

  autocompleteItemTestIdPrefix = "autocomplete-item";

  viewAllSectionTestId = "view-all-results";

  filterBaseButtonClearTestId = "button-clear";

  filterSearchInputTestId = "dropdown-search-bar";

  filterUpdateButtonTestId = "footer-button-update";

  filterCancelButtonTestId = "footer-button-cancel";

  noResultsFoundTestId = "no-results-found";

  noResultsFoundClearButtonTestId = "no-results-found-button-clear";

  waitForSearchBar() {
    cy.getWithTestId(this.searchBarWrapperTestId).should("be.visible");
  }

  click() {
    this.waitForSearchBar();
    cy.get('[id="v2-search-bar"]').within(() => {
      cy.clickOptionWithTestId(this.searchBarTestId);
    });
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

  getEntityResponseItem(entityUrn) {
    return cy.getWithTestId(this.getTestIdForEntityResponseItem(entityUrn));
  }

  ensureEntityInResponse(entityUrn) {
    this.getEntityResponseItem(entityUrn).should("be.visible");
  }

  ensureEntityNotInResponse(entityUrn) {
    this.getEntityResponseItem(entityUrn).should("not.exist");
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
  }

  ensureNoResultsFoundStateHasClearButton() {
    cy.getWithTestId(this.noResultsFoundClearButtonTestId).should("be.visible");
  }

  ensureNoResultsFoundStateHasNoClearButton() {
    cy.getWithTestId(this.noResultsFoundClearButtonTestId).should("not.exist");
  }

  ensureMatchedFieldHasText(entityUrn, field, value) {
    this.getEntityResponseItem(entityUrn).within(() => {
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
}
