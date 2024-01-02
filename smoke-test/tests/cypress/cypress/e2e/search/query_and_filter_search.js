const datasetNames = {
  dashboardsType: "Baz Dashboard",
  pipelinesType: "Users",
  MlmoduleType: "scienceModel",
  glossaryTermsType: "AccountBalance",
  tags: "Baz Chart 1",
  hivePlatform: "datahub_db",
  airflowPlatform: "User Creations",
  awsPlatform: "project/root/events/logging_events_bckp",
  hdfsPlatform: "SampleHdfsDataset"
};

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
  cy.get('.ant-pagination-next').scrollIntoView().should('be.visible');
};

const verifyFilteredEntity = (text) => {
  cy.get('.ant-typography').contains(text).should('be.visible');
};

 describe("auto-complete dropdown, filter plus query search test", () => {
  it("verify the 'filter by' section + query (result in search page with query applied + filter applied)", () => {
  
   // Platform query plus filter test
    cy.loginWithCredentials();      
    cy.visit('/');
    searchToExecute("*");

   // Filter by entity - Type

    // Dashboard
   selectFilteredEntity("Type", "Dashboards", "filter__entityType");  
   cy.clickOptionWithText(datasetNames.dashboardsType);
   verifyFilteredEntity('Dashboard');
   searchToExecute("*");

    // Ml models
   selectFilteredEntity("Type", "ML Models", "filter__entityType");  
   cy.clickOptionWithText(datasetNames.MlmoduleType);
   verifyFilteredEntity('ML Model');
   searchToExecute("*");

   // Pipeline
   selectFilteredEntity("Type", "Pipelines", "filter__entityType");  
   cy.clickOptionWithText(datasetNames.pipelinesType);
   verifyFilteredEntity('Pipeline');
   searchToExecute('*')

   // Filter by entity - Platform

   // Hive
   selectFilteredEntity("Platform", "Hive", "filter_platform");
    cy.clickOptionWithText(datasetNames.hivePlatform);
    verifyFilteredEntity('Hive');
    searchToExecute('*')
  
    // Airflow
    selectFilteredEntity("Platform", "Airflow", "filter_platform");
    cy.clickOptionWithText(datasetNames.airflowPlatform);
    verifyFilteredEntity('Airflow');
    searchToExecute('*')

    // AWS S3
    selectFilteredEntity("Platform", "AWS S3", "filter_platform");
    cy.clickOptionWithText(datasetNames.awsPlatform);
    verifyFilteredEntity('AWS S3');
    searchToExecute('*')

    // HDFS
    selectFilteredEntity("Platform", "HDFS", "filter_platform");
    cy.clickOptionWithText(datasetNames.hdfsPlatform);
    verifyFilteredEntity('HDFS');
    searchToExecute('*')

    // Glossary Terms
    selectFilteredEntity("Type", "Glossary Terms", "filter__entityType");
    cy.clickOptionWithText(datasetNames.glossaryTermsType);
    verifyFilteredEntity('Glossary Term');
    searchToExecute('*')
    
    // Tag
    selectFilteredEntity("Tag", "Legacy", "filter_tags");
    cy.clickOptionWithText(datasetNames.tags);
    cy.mouseover('[data-testid="tag-Legacy"]');
    verifyFilteredEntity('Legacy');
   });
});
