import { aliasQuery, hasOperationName } from "../utils";

describe("attribute list adding tags and terms", () => {
  let businessAttributeEntityEnabled;

  beforeEach(() => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setBusinessAttributeFeatureFlag = () => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.reply((res) => {
          businessAttributeEntityEnabled =
            res.body.data.appConfig.featureFlags.businessAttributeEntityEnabled;
          return res;
        });
      }
    }).as("apiCall");
  };

  it("can create and add a tag to business attribute and visit new tag page", () => {
    setBusinessAttributeFeatureFlag();
    cy.login();
    cy.visit("/business-attribute");
    cy.wait("@apiCall").then(() => {
      if (!businessAttributeEntityEnabled) {
        return;
      }
      cy.wait(3000);
      cy.waitTextVisible("Business Attribute");
      cy.wait(3000);

      cy.mouseover('[data-testid="schema-field-cypressTestAttribute-tags"]');
      cy.get('[data-testid="schema-field-cypressTestAttribute-tags"]').within(
        () => cy.contains("Add Tags").click(),
      );

      cy.enterTextInTestId("tag-term-modal-input", "CypressAddTagToAttribute");

      cy.contains("Create CypressAddTagToAttribute").click({ force: true });

      cy.get("textarea").type("CypressAddTagToAttribute Test Description");

      cy.contains(/Create$/).click({ force: true });

      // wait a breath for elasticsearch to index the tag being applied to the business attribute- if we navigate too quick ES
      // wont know and we'll see applied to 0 entities
      cy.wait(3000);

      // go to tag drawer
      cy.contains("CypressAddTagToAttribute").click({ force: true });

      cy.wait(3000);

      // Click the Tag Details to launch full profile
      cy.contains("Tag Details").click({ force: true });

      cy.wait(3000);

      // title of tag page
      cy.contains("CypressAddTagToAttribute");

      // description of tag page
      cy.contains("CypressAddTagToAttribute Test Description");

      cy.wait(3000);
      // used by panel - click to search
      cy.contains("1 Business Attributes").click({ force: true });

      // verify business attribute shows up in search now
      cy.contains("of 1 result").click({ force: true });
      cy.contains("cypressTestAttribute").click({ force: true });
      cy.get('[data-testid="tag-CypressAddTagToAttribute"]').within(() =>
        cy.get("span[aria-label=close]").click(),
      );
      cy.contains("Yes").click();

      cy.contains("CypressAddTagToAttribute").should("not.exist");

      cy.goToTag(
        "urn:li:tag:CypressAddTagToAttribute",
        "CypressAddTagToAttribute",
      );
      cy.deleteFromDropdown();
    });
  });

  it("can add and remove terms from a business attribute", () => {
    setBusinessAttributeFeatureFlag();
    cy.login();
    cy.visit(
      "/business-attribute/" + "urn:li:businessAttribute:cypressTestAttribute",
    );
    cy.wait("@apiCall").then(() => {
      if (!businessAttributeEntityEnabled) {
        return;
      }
      cy.wait(3000);
      cy.waitTextVisible("cypressTestAttribute");
      cy.wait(3000);
      cy.clickOptionWithText("Add Terms");
      cy.selectOptionInTagTermModal("CypressTerm");
      cy.contains("CypressTerm");

      cy.goToBusinessAttributeList();
      cy.get(
        '[data-testid="schema-field-cypressTestAttribute-terms"]',
      ).contains("CypressTerm");

      cy.get('[data-testid="schema-field-cypressTestAttribute-terms"]').within(
        () =>
          cy
            .get("span[aria-label=close]")
            .trigger("mouseover", { force: true })
            .click({ force: true }),
      );
      cy.contains("Yes").click({ force: true });

      cy.get('[data-testid="schema-field-cypressTestAttribute-terms"]')
        .contains("CypressTerm")
        .should("not.exist");
    });
  });
});
