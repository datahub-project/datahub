const datasetNames = {
  dashboardsType: "Baz Dashboard",
  pipelinesType: "Users",
  MlmoduleType: "cypress-model",
  glossaryTermsType: "CypressColumnInfoType",
  tags: "some-cypress-feature-1",
  hivePlatform: "cypress_logging_events",
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
  
  beforeEach(() => {
    cy.loginWithCredentials();      
    cy.visit('/');
  });

  it.skip("Verify the 'filter by type' section + query", () => {

    //Dashboard
    searchToExecute("*");
    selectFilteredEntity("Type", "Dashboards", "filter__entityType");  
    cy.clickOptionWithText(datasetNames.dashboardsType);
    verifyFilteredEntity('Dashboard');

    //Ml Models
    searchToExecute("*");
    selectFilteredEntity("Type", "ML Models", "filter__entityType");  
    cy.clickOptionWithText(datasetNames.MlmoduleType);
    verifyFilteredEntity('ML Model');

    //Piplines
    searchToExecute("*");
    selectFilteredEntity("Type", "Pipelines", "filter__entityType");  
    cy.clickOptionWithText(datasetNames.pipelinesType);
    verifyFilteredEntity('Pipeline');
   
  });

  it("Verify the 'filter by Glossary term' section + query", () => {
   
  //Glossary Term  
  searchToExecute("*");
  selectFilteredEntity("Type", "Glossary Terms", "filter__entityType");
  cy.clickOptionWithText(datasetNames.glossaryTermsType);
  verifyFilteredEntity('Glossary Term');
});

  it("Verify the 'filter by platform' section + query", () => {
   
    //Hive
    searchToExecute("*");
    selectFilteredEntity("Platform", "Hive", "filter_platform");
    cy.clickOptionWithText(datasetNames.hivePlatform);
    verifyFilteredEntity('Hive');
    
    //AWS S3
    searchToExecute("*");
    selectFilteredEntity("Platform", "AWS S3", "filter_platform");
    cy.clickOptionWithText(datasetNames.awsPlatform);
    verifyFilteredEntity('AWS S3');
    
    //HDFS
    searchToExecute("*");
    selectFilteredEntity("Platform", "HDFS", "filter_platform");
    cy.clickOptionWithText(datasetNames.hdfsPlatform);
    verifyFilteredEntity('HDFS');

    //Airflow
    searchToExecute("*");
    selectFilteredEntity("Platform", "Airflow", "filter_platform");
    cy.clickOptionWithText(datasetNames.airflowPlatform);
    verifyFilteredEntity('Airflow');
  });

  it("Verify the 'filter by tag' section + query", () => {
    
    //CypressFeatureTag
    searchToExecute("*");
    selectFilteredEntity("Tag", "CypressFeatureTag", "filter_tags");
    cy.clickOptionWithText(datasetNames.tags);
    cy.mouseover('[data-testid="tag-CypressFeatureTag"]');
    verifyFilteredEntity('Feature');
  });
});
