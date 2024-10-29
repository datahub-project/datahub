import { aliasQuery, hasOperationName } from "../utils";

describe("businessAttribute", () => {
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

  it("go to business attribute page, create attribute ", () => {
    const urn =
      "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
    const businessAttribute = "CypressBusinessAttribute";
    const datasetName = "cypress_logging_events";
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
      cy.clickOptionWithText("Create Business Attribute");
      cy.addBusinessAttributeViaModal(
        businessAttribute,
        "Create Business Attribute",
        businessAttribute,
        "create-business-attribute-button",
      );

      cy.wait(3000);
      cy.goToBusinessAttributeList();

      cy.wait(3000);
      cy.contains(businessAttribute).should("be.visible");

      cy.addAttributeToDataset(urn, datasetName, businessAttribute);

      cy.get(
        '[data-testid="schema-field-event_name-businessAttribute"]',
      ).within(() =>
        cy
          .get("span[aria-label=close]")
          .trigger("mouseover", { force: true })
          .click({ force: true }),
      );
      cy.contains("Yes").click({ force: true });

      cy.get('[data-testid="schema-field-event_name-businessAttribute"]')
        .contains("CypressBusinessAttribute")
        .should("not.exist");

      cy.goToBusinessAttributeList();
      cy.clickOptionWithText(businessAttribute);
      cy.deleteFromDropdown();

      cy.goToBusinessAttributeList();
      cy.ensureTextNotPresent(businessAttribute);
    });
  });

  it("Inheriting tags and terms from business attribute to dataset ", () => {
    const urn =
      "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
    const businessAttribute = "CypressAttribute";
    const datasetName = "cypress_logging_events";
    const term = "CypressTerm";
    const tag = "Cypress";
    setBusinessAttributeFeatureFlag();
    cy.login();
    cy.visit(`/dataset/${urn}`);
    cy.wait("@apiCall").then(() => {
      if (!businessAttributeEntityEnabled) {
        return;
      }
      cy.wait(5000);
      cy.waitTextVisible(datasetName);
      cy.clickOptionWithText("event_name");
      cy.contains("Business Attribute");
      cy.get(
        '[data-testid="schema-field-event_name-businessAttribute"]',
      ).within(() => cy.contains("Add Attribute").click());
      cy.selectOptionInAttributeModal(businessAttribute);
      cy.contains(businessAttribute);
      cy.contains(term);
      cy.contains(tag);
    });
  });

  it("can visit related entities", () => {
    const businessAttribute = "CypressAttribute";
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
      cy.clickOptionWithText(businessAttribute);
      cy.clickOptionWithText("Related Entities");
      // cy.visit("/business-attribute/urn:li:businessAttribute:37c81832-06e0-40b1-a682-858e1dd0d449/Related%20Entities");
      // cy.wait(5000);
      cy.contains("of 0").should("not.exist");
      cy.contains(/of [0-9]+/);
    });
  });

  it("can search related entities by query", () => {
    setBusinessAttributeFeatureFlag();
    cy.login();
    cy.visit(
      "/business-attribute/urn:li:businessAttribute:37c81832-06e0-40b1-a682-858e1dd0d449/Related%20Entities",
    );
    cy.wait("@apiCall").then(() => {
      if (!businessAttributeEntityEnabled) {
        return;
      }
      cy.get('[placeholder="Filter entities..."]')
        .click()
        .type("event_n{enter}");
      cy.wait(5000);
      cy.contains("of 0").should("not.exist");
      cy.contains(/of 1/);
      cy.contains("event_name");
    });
  });

  it("remove business attribute from dataset", () => {
    const urn =
      "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
    const datasetName = "cypress_logging_events";
    setBusinessAttributeFeatureFlag();
    cy.login();
    cy.visit(`/dataset/${urn}`);
    cy.wait("@apiCall").then(() => {
      if (!businessAttributeEntityEnabled) {
        return;
      }
      cy.wait(5000);
      cy.waitTextVisible(datasetName);

      cy.wait(3000);
      cy.get("body").then(($body) => {
        if ($body.find('button[aria-label="Close"]').length > 0) {
          cy.get('button[aria-label="Close"]').click();
        }
      });
      cy.clickOptionWithText("event_name");
      cy.get(
        '[data-testid="schema-field-event_name-businessAttribute"]',
      ).within(() =>
        cy
          .get("span[aria-label=close]")
          .trigger("mouseover", { force: true })
          .click({ force: true }),
      );
      cy.contains("Yes").click({ force: true });

      cy.get('[data-testid="schema-field-event_name-businessAttribute"]')
        .contains("CypressAttribute")
        .should("not.exist");
    });
  });

  it("update the data type of a business attribute", () => {
    const businessAttribute = "cypressTestAttribute";
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

      cy.clickOptionWithText(businessAttribute);

      cy.get('[data-testid="edit-data-type-button"]').within(() =>
        cy
          .get("span[aria-label=edit]")
          .trigger("mouseover", { force: true })
          .click({ force: true }),
      );

      cy.get('[data-testid="add-data-type-option"]')
        .get(".ant-select-selection-search-input")
        .click({ multiple: true });

      cy.get(".ant-select-item-option-content").contains("STRING").click();

      cy.contains("STRING");
    });
  });
});
