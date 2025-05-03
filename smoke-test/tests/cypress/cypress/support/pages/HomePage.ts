class HomePage {
  /**
   * Visits the home page
   */
  visit() {
    cy.visit("/");
    return this;
  }

  /**
   * Checks if the home page content container exists
   */
  shouldHaveContentContainer() {
    cy.get('[data-testid="home-page-content-container"]').should("exist");
    return this;
  }

  /**
   * Checks if the navigation menu exists
   */
  shouldHaveNavigationMenu() {
    cy.get('[data-testid="nav-menu-links"]').should("exist");
    return this;
  }

  /**
   * Checks if the SVG content exists
   */
  shouldHaveSvgContent() {
    cy.get('[xmlns="http://www.w3.org/2000/svg"]').should("exist");
    return this;
  }
}

export const homePage = new HomePage();
