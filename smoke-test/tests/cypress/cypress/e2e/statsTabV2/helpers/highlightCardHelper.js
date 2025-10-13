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
