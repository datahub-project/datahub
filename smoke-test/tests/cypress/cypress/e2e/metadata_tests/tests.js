import { aliasQuery, hasOperationName } from "../utils";

const urn =
  "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)";
const datasetName = "SampleCypressHdfsDataset";
const number = Math.floor(Math.random() * 100000);
const testName = `Cypress Tag Test ${number}`;
const testDescription = "Cyprress test description";

const setTestsConfigFlag = (isOn) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.reply((res) => {
        // Modify the response body directly
        res.body.data.appConfig.testsConfig.enabled = isOn;
      });
    }
  });
};

const clickFilterAndFacet = () => {
  cy.clickOptionWithSpecificClass(".anticon-filter", 0);
  cy.clickOptionWithTestId("facet-_entityType-DATASET");
  cy.clickOptionWithSpecificClass(".anticon-filter", 0);
};

describe("create, edit and remove metadata test", () => {
  beforeEach(() => {
    cy.on("uncaught:exception", (err, runnable) => false);
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  it("create new test at governance > tests, edit a test to make if fail, remove test", () => {
    // create new test at governance > tests, test conditions and save the test
    setTestsConfigFlag(true);
    cy.loginWithCredentials();
    cy.goToTestsList();
    cy.clickOptionWithText("Create");
    cy.waitTextVisible("New Metadata Test");
    // select data assets
    cy.contains("Datasets, Dashboards, Charts...").prev().click();
    cy.get(".rc-virtual-list").find("div").contains("Datasets").click();
    cy.get("body").click();
    cy.clickOptionWithText("+ Add");
    cy.clickOptionWithText("Property");
    cy.get('[role="dialog"] [type="search"]').eq(1).click();
    cy.clickOptionWithText("Urn");
    cy.get('[role="dialog"] [type="search"]').eq(2).click();
    cy.clickOptionWithText("Equals");
    cy.get('[role="dialog"] [type="search"]').eq(3).type(urn);
    cy.clickOptionWithText("Next");
    // define conditions
    cy.waitTextVisible("Define your test conditions");
    cy.clickOptionWithText("+ Add");
    cy.clickOptionWithText("Property");
    cy.focused().click();
    cy.clickOptionWithText("Tags");
    cy.get('[role="dialog"] [type="search"]').eq(1).click();
    cy.clickOptionWithText("Contains Any");
    cy.get('[role="dialog"] [type="search"]').eq(2).click().type("Cypress");
    cy.get(".rc-virtual-list").find("div").contains("Cypress").click();
    // test conditions
    cy.clickOptionWithText("Test Conditions");
    clickFilterAndFacet();
    cy.get('[role="dialog"] [data-testid="search-input"]').type("hdfs");
    cy.waitTextVisible(datasetName);
    cy.clickOptionWithText("Run Test");
    cy.waitTextVisible("Passed");
    cy.get('[role="dialog"] [data-testid="search-input"]').clear().type("hive");
    cy.get('[href^="/dataset')
      .should("be.visible")
      .contains("SampleCypressHiveDataset");
    cy.clickFirstOptionWithText("Run Test");
    cy.waitTextVisible("Not selected");
    cy.clickOptionWithText("Close");
    // finish up
    cy.clickOptionWithText("Next");
    cy.get('[placeholder="A name for your test"]').type(testName);
    cy.get('[placeholder="The description for your test"]').type(
      testDescription,
    );
    cy.clickOptionWithText("Save");
    cy.waitTextVisible("Successfully created Test!");
    cy.waitTextVisible(testName);
    cy.waitTextVisible(testDescription);
    cy.reload();
    cy.get(".ant-card").first().contains("1 passing");
    // edit the test to make it fail, verify the result, save test
    cy.contains(testName).click();
    cy.waitTextVisible("Edit Metadata Test");
    cy.clickOptionWithText("Next");
    cy.get('[role="dialog"] [type="search"]').eq(2).click();
    cy.get(".rc-virtual-list").find("div").contains("Cypress").click();
    cy.get(".rc-virtual-list").find("div").contains("TagToPropose").click();
    cy.get("body").click();
    // test conditions, verify that test fails
    cy.clickOptionWithText("Test Conditions");
    clickFilterAndFacet();
    cy.get('[role="dialog"] [data-testid="search-input"]').type("hdfs");
    cy.waitTextVisible(datasetName);
    cy.clickOptionWithText("Run Test");
    cy.waitTextVisible("Failed");
    cy.clickOptionWithText("Close");
    // save edited test
    cy.clickOptionWithText("Next");
    cy.clickOptionWithText("Save");
    cy.waitTextVisible("Successfully updated Test!");
    cy.waitTextVisible(testName);
    cy.reload();
    cy.get(".ant-card").first().contains("1 failing");
    // delete a test
    cy.get('[data-testid="test-more-button-0"]').click();
    cy.clickOptionWithText("Delete");
    cy.waitTextVisible("Confirm Test Removal");
    cy.clickOptionWithText("Yes");
    cy.waitTextVisible("Removed test.");
    cy.reload();
    cy.ensureTextNotPresent(testName);
  });
});
