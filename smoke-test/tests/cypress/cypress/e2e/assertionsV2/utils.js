/**
 * Shared utilities for assertionV2 tests
 */

/**
 * Click on an element by selector
 */
export const clickElement = (locator) => {
  cy.get(locator).click();
};

/**
 * Close the assertion profile drawer by clicking on the mask overlay.
 * More reliable than clicking body since body click might land on drawer content itself.
 */
export const closeDrawer = () => {
  cy.get(".ant-drawer-mask").click({ force: true });
  cy.get(".ant-drawer-mask").should("not.exist");
};
