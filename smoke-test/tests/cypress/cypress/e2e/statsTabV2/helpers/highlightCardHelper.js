/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export class HighlightCardHelper {
  constructor(cardName) {
    this.cardName = cardName;
  }

  getCard() {
    return cy.getWithTestId(`${this.cardName}-card`);
  }

  ensureHasValue(value) {
    this.getCard().within(() => {
      cy.getWithTestId("title").contains(value);
    });
  }

  ensureHasViewButton() {
    this.getCard().within(() => {
      cy.getWithTestId("view-button").should("exist");
    });
  }

  ensureHasNoValue() {
    this.getCard().within(() => {
      cy.getWithTestId("no-data").should("exist");
      cy.getWithTestId("view-button").should("not.exist");
    });
  }
}
