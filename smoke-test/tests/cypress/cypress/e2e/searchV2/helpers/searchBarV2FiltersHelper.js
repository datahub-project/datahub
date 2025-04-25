export class SearchBarV2FiltersHelper {
  filterBaseButtonClearTestId = "button-clear";
  filterSearchInputTestId = "dropdown-search-bar";
  filterUpdateButtonTestId = "footer-button-update";
  filterCancellButtonTestId = "footer-button-cancel";

  click(filterName) {
    cy.clickOptionWithTestId(this.getTestIdForFilterBase(filterName));
  }

  getDropdown(filterName) {
    return cy.getWithTestId(this.getTestIdForFilterDropdown(filterName));
  }

  getFilter(filterName) {
    return cy.getWithTestId(this.getTestIdForFilterBase(filterName));
  }

  search(filterName, text) {
    this.getDropdown(filterName).within(() => {
      cy.clearTextInTestId(this.filterSearchInputTestId);
      cy.enterTextInTestId(this.filterSearchInputTestId, text);
      this.waitForApiResponse(); // wait for filtering response
    });
  }

  selectOption(filterName, text) {
    this.getDropdown(filterName).within(() => {
      cy.contains(text).click();
    });
  }

  update(filterName) {
    this.getDropdown(filterName).within(() => {
      cy.clickOptionWithTestId(this.filterUpdateButtonTestId);
    });
  }

  cancel(filterName) {
    this.getDropdown(filterName).within(() => {
      cy.clickOptionWithTestId(this.filterCancellButtonTestId);
    });
  }

  apply(filterName, values) {
    this.click(filterName);

    values.forEach((value) => {
      this.search(filterName, value);
      this.selectOption(filterName, value);
    });

    this.update(filterName);
    this.waitForApiResponse(); // wait for search bar's response after aplying filter
  }

  clear(filterName) {
    this.getFilter(filterName).within(() => {
      cy.clickOptionWithTestId(this.filterBaseButtonClearTestId);
    });
    this.waitForApiResponse(); // wait for search bar's response after clearing filter
  }

  ensureValuesSelected(filterName, values) {
    this.click(filterName);

    values.forEach((value) => {
      this.search(filterName, value);
      this.getDropdown(filterName).within(() => {
        cy.contains(value).within(() =>
          cy.get("input[type='checkbox']").should("be.checked"),
        );
      });
    });

    this.cancel(filterName);
  }

  ensureValuesNotSelected(filterName, values) {
    this.click(filterName);

    values.forEach((value) => {
      this.search(filterName, value);
      this.getDropdown(filterName).within(() => {
        cy.contains(value).within(() =>
          cy.get("input[type='checkbox']").should("not.be.checked"),
        );
      });
    });

    this.cancel(filterName);
  }

  getTestIdForFilterBase(filterName) {
    return `filter-${filterName}-base`;
  }

  getTestIdForFilterDropdown(filterName) {
    return `filter-${filterName}-dropdown`;
  }

  waitForApiResponse() {
    cy.wait(1000);
  }
}
