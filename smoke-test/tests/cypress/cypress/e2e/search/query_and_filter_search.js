const searchToExecute = (value) => {
  cy.get("input[data-testid=search-input]").eq(0).type(`${value}{enter}`);
  cy.waitTextPresent("Type");
};

const selectFilteredEntity = (textToClick, entity, url) => {
  cy.get(`[data-testid=filter-dropdown-${textToClick}]`).click({ force: true });
  cy.get(`[data-testid="filter-option-${entity}"]`).click({ force: true });
  cy.get("[data-testid=update-filters]").click({ force: true });
  cy.url().should("include", `${url}`);
  cy.get("[data-testid=update-filters]").should("not.be.visible");
  cy.get(".ant-pagination-next").scrollIntoView().should("be.visible");
};

const verifyFilteredEntity = (text) => {
  cy.get(".ant-typography").contains(text).should("be.visible");
};

const clickAndVerifyEntity = (entity) => {
  cy.get('[class*="entityUrn-urn"]')
    .first()
    .find('a[href*="urn:li"] span[class^="ant-typography"]')
    .last()
    .invoke("text")
    .then((text) => {
      cy.contains(text).click();
      verifyFilteredEntity(text);
      verifyFilteredEntity(entity);
    });
};

describe("auto-complete dropdown, filter plus query search test", () => {
  beforeEach(() => {
    cy.loginWithCredentials();
    cy.visit("/");
  });

  it("Verify the 'filter by type' section + query", () => {
    // Dashboard
    searchToExecute("*");
    selectFilteredEntity("Type", "Dashboards", "filter__entityType");
    clickAndVerifyEntity("Dashboard");

    // Ml Models
    searchToExecute("*");
    selectFilteredEntity("Type", "ML Models", "filter__entityType");
    clickAndVerifyEntity("ML Model");

    // Piplines
    searchToExecute("*");
    selectFilteredEntity("Type", "Pipelines", "filter__entityType");
    clickAndVerifyEntity("Pipeline");
  });

  it("Verify the 'filter by Glossary term' section + query", () => {
    // Glossary Term
    searchToExecute("*");
    selectFilteredEntity("Type", "Glossary Terms", "filter__entityType");
    clickAndVerifyEntity("Glossary Term");
  });

  it("Verify the 'filter by platform' section + query", () => {
    // Hive
    searchToExecute("*");
    selectFilteredEntity("Platform", "Hive", "filter_platform");
    clickAndVerifyEntity("Hive");

    // HDFS
    searchToExecute("*");
    selectFilteredEntity("Platform", "HDFS", "filter_platform");
    clickAndVerifyEntity("HDFS");

    // Airflow
    searchToExecute("*");
    selectFilteredEntity("Platform", "Airflow", "filter_platform");
    clickAndVerifyEntity("Airflow");
  });

  it("Verify the 'filter by tag' section + query", () => {
    // CypressFeatureTag
    searchToExecute("*");
    selectFilteredEntity("Tag", "CypressFeatureTag", "filter_tags");
    clickAndVerifyEntity("Tags");
    cy.mouseover('[data-testid="tag-CypressFeatureTag"]');
    verifyFilteredEntity("CypressFeatureTag");
  });
});
