import { aliasQuery, hasOperationName } from "../utils";

describe("mutations", () => {
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

  before(() => {
    // warm up elastic by issuing a `*` search
    cy.login();
    // Commented below function, and used individual commands below with wait
    // cy.goToStarSearchList();
    cy.visit("/search?query=%2A");
    cy.wait(3000);
    cy.waitTextVisible("Showing");
    cy.waitTextVisible("results");
    cy.wait(2000);
    cy.get("body").then(($body) => {
      if ($body.find('button[aria-label="Close"]').length > 0) {
        cy.get('button[aria-label="Close"]').click();
      }
    });
    cy.wait(2000);
  });

  it("can create and add a tag to dataset and visit new tag page", () => {
    // cy.deleteUrn("urn:li:tag:CypressTestAddTag");
    cy.login();
    cy.goToDataset(
      "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)",
      "cypress_logging_events",
    );
    cy.get("body").then(($body) => {
      if ($body.find('button[aria-label="Close"]').length > 0) {
        cy.get('button[aria-label="Close"]').click();
      }
    });
    cy.contains("Add Tag").click({ force: true });

    cy.enterTextInTestId("tag-term-modal-input", "CypressTestAddTag");

    cy.contains("Create CypressTestAddTag").click({ force: true });

    cy.get("textarea").type("CypressTestAddTag Test Description");

    cy.contains(/Create$/).click({ force: true });

    // wait a breath for elasticsearch to index the tag being applied to the dataset- if we navigate too quick ES
    // wont know and we'll see applied to 0 entities
    cy.wait(2000);

    // go to tag drawer
    cy.contains("CypressTestAddTag").click({ force: true });

    cy.wait(2000);

    // Click the Tag Details to launch full profile
    cy.contains("Tag Details").click({ force: true });

    cy.wait(2000);

    // title of tag page
    cy.contains("CypressTestAddTag");

    cy.wait(2000);
    // description of tag page
    cy.contains("CypressTestAddTag Test Description");

    // used by panel - click to search
    cy.wait(3000);
    cy.contains("1 Datasets").click({ force: true });

    // verify dataset shows up in search now
    cy.contains("of 1 result").click({ force: true });
    cy.contains("cypress_logging_events").click({ force: true });
    cy.get('[data-testid="tag-CypressTestAddTag"]').within(() =>
      cy.get("span[aria-label=close]").click(),
    );
    cy.contains("Yes").click();

    cy.contains("CypressTestAddTag").should("not.exist");
    // cy.deleteUrn("urn:li:tag:CypressTestAddTag");
  });

  it("can add and remove terms from a dataset", () => {
    cy.login();
    cy.addTermToDataset(
      "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)",
      "cypress_logging_events",
      "CypressTerm",
    );

    cy.get(
      'a[href="/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressTerm"]',
    ).within(() => cy.get("span[aria-label=close]").click());
    cy.contains("Yes").click();

    cy.contains("CypressTerm").should("not.exist");
  });

  it("can add and remove tags from a dataset field", () => {
    cy.login();
    cy.viewport(2000, 800);
    cy.goToDataset(
      "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)",
      "cypress_logging_events",
    );
    cy.clickOptionWithText("event_name");
    cy.get('[data-testid="schema-field-event_name-tags"]').within(() =>
      cy.contains("Add Tag").click(),
    );

    cy.enterTextInTestId("tag-term-modal-input", "CypressTestAddTag2");

    cy.contains("Create CypressTestAddTag2").click({ force: true });

    cy.get("textarea").type("CypressTestAddTag2 Test Description");

    cy.contains(/Create$/).click({ force: true });

    // wait a breath for elasticsearch to index the tag being applied to the dataset- if we navigate too quick ES
    // wont know and we'll see applied to 0 entities
    cy.wait(2000);

    // go to tag drawer
    cy.contains("CypressTestAddTag2").click({ force: true });

    cy.wait(2000);

    // Click the Tag Details to launch full profile
    cy.contains("Tag Details").click({ force: true });

    cy.wait(2000);

    // title of tag page
    cy.contains("CypressTestAddTag2");

    // description of tag page
    cy.contains("CypressTestAddTag2 Test Description");

    // used by panel - click to search
    cy.wait(3000);
    cy.contains("1 Datasets").click();

    // verify dataset shows up in search now
    cy.contains("of 1 result").click();
    cy.contains("cypress_logging_events").click();
    cy.clickOptionWithText("event_name");
    cy.get('[data-testid="schema-field-event_name-tags"]').within(() =>
      cy
        .get("span[aria-label=close]")
        .trigger("mouseover", { force: true })
        .click({ force: true }),
    );
    cy.contains("Yes").click({ force: true });

    cy.contains("CypressTestAddTag2").should("not.exist");

    // cy.deleteUrn("urn:li:tag:CypressTestAddTag2");
  });

  it("can add and remove terms from a dataset field", () => {
    cy.login();
    // make space for the glossary term column
    cy.viewport(2000, 800);
    cy.goToDataset(
      "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)",
      "cypress_logging_events",
    );
    cy.clickOptionWithText("event_name");
    cy.get('[data-testid="schema-field-event_name-terms"]').within(() =>
      cy.contains("Add Term").click({ force: true }),
    );

    cy.selectOptionInTagTermModal("CypressTerm");

    cy.contains("CypressTerm");

    cy.get('[data-testid="schema-field-event_name-terms"]').within(() =>
      cy
        .get("span[aria-label=close]")
        .trigger("mouseover", { force: true })
        .click({ force: true }),
    );
    cy.contains("Yes").click({ force: true });

    cy.contains("CypressTerm").should("not.exist");
  });

  it("can add and remove business attribute from a dataset field", () => {
    setBusinessAttributeFeatureFlag();
    cy.login();
    // make space for the glossary term column
    cy.viewport(2000, 800);
    cy.visit(
      "/dataset/" +
        "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)",
    );
    cy.wait("@apiCall").then(() => {
      if (!businessAttributeEntityEnabled) {
        return;
      }
      cy.wait(5000);
      cy.waitTextVisible("cypress_logging_events");
      cy.clickOptionWithText("event_data");
      cy.get(
        '[data-testid="schema-field-event_data-businessAttribute"]',
      ).trigger("mouseover", { force: true });
      cy.get(
        '[data-testid="schema-field-event_data-businessAttribute"]',
      ).within(() => cy.contains("Add Attribute").click({ force: true }));

      cy.selectOptionInAttributeModal("cypressTestAttribute");
      cy.wait(2000);
      cy.contains("cypressTestAttribute");

      cy.get(
        '[data-testid="schema-field-event_data-businessAttribute"]',
      ).within(() =>
        cy
          .get("span[aria-label=close]")
          .trigger("mouseover", { force: true })
          .click({ force: true }),
      );
      cy.contains("Yes").click({ force: true });

      cy.contains("cypressTestAttribute").should("not.exist");
    });
  });
});
