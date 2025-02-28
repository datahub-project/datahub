import { aliasQuery, hasOperationName } from "../utils";

const urn =
  "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)";
const datasetName = "SampleCypressHdfsDataset";
const number = Math.floor(Math.random() * 100000);
const testName = `Cypress Tag Test ${number}`;
const testDescription = "Cyprress test description";

describe("create, edit and remove metadata test", () => {
  beforeEach(() => {
    cy.on("uncaught:exception", (err, runnable) => false);
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setTestsConfigFlag = (isOn) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.reply((res) => {
          // Modify the response body directly
          res.body.data.appConfig.testsConfig.enabled = isOn;
          res.body.data.appConfig.featureFlags.themeV2Enabled = true;
          res.body.data.appConfig.featureFlags.themeV2Default = true;
        });
      }
    });
  };

  it("create new test at governance > tests, edit a test to make if fail, remove test", () => {
    // create new test at governance > tests, test conditions and save the test
    setTestsConfigFlag(true);
    cy.visitWithLogin("/tests");
    cy.wait(3000); // Page seems to refresh otherwise and close create modal
    cy.contains("Create").first().click({ force: true });
    cy.waitTextVisible("New Metadata Test");
    // select data assets
    cy.get('[data-testid="entity-type-select"] > .ant-select-selector').click();
    cy.get(".rc-virtual-list").find("div").contains("Datasets").click();
    cy.get("body").click();
    cy.clickOptionWithText("+ Add");
    cy.clickOptionWithText("Property");
    cy.get('[role="dialog"] [type="search"]').eq(1).click();
    cy.clickOptionWithText("Urn");
    cy.get('[role="dialog"] [type="search"]').eq(2).click();
    cy.clickOptionWithText("Equals");
    cy.get('[role="dialog"] [type="search"]').eq(3).type(urn);
    cy.get('[data-testid="modal-next-button"]').click();
    // define conditions
    cy.waitTextVisible("Define your test conditions");
    cy.clickOptionWithText("+ Add");
    cy.clickOptionWithText("Property");
    cy.focused().click();
    cy.clickOptionWithText("Tags");
    cy.get('[role="dialog"] [type="search"]').eq(1).click();
    cy.clickOptionWithText("Contains Any");
    cy.get('[role="dialog"] [type="search"]')
      .eq(2)
      .click()
      .type("Cypress")
      .wait(500);
    cy.get(".rc-virtual-list").find("div").contains("Cypress").click();
    // test conditions
    cy.clickOptionWithText("Test Conditions");
    cy.get('[role="dialog"] [data-testid="search-input"]').type("hdfs");
    cy.waitTextVisible(datasetName);
    cy.clickOptionWithText("Run Test");
    cy.waitTextVisible("Passed");
    cy.get('[role="dialog"] [data-testid="search-input"]')
      .clear()
      .type("SampleCypressHiveDataset");
    cy.get('[href*="/dataset')
      .should("be.visible")
      .contains("SampleCypressHiveDataset");
    cy.contains("Run Test").click();
    cy.waitTextVisible("Not selected");
    cy.clickOptionWithText("Close");
    // finish up
    cy.get('[data-testid="modal-next-button"]').click();
    cy.get('[placeholder="A name for your test"]').type(testName);
    cy.get('[placeholder="The description for your test"]').type(
      testDescription,
    );
    cy.clickOptionWithText("Save");
    cy.waitTextVisible("Successfully created Test!");
    cy.waitTextVisible(testName);
    cy.waitTextVisible(testDescription);
    cy.reload();
    cy.waitTextVisible("No results found");
    // edit the test to make it fail, verify the result, save test
    cy.visit("/tests");
    cy.contains(testName).click();
    cy.waitTextVisible("Edit Metadata Test");
    cy.get('[data-testid="modal-next-button"]').click();
    cy.get('[role="dialog"] [type="search"]').eq(2).click();
    cy.get(".rc-virtual-list").find("div").contains("Cypress").click();
    cy.get(".rc-virtual-list").find("div").contains("TagToPropose").click();
    cy.clickOptionWithText("Edit Metadata Test");
    // test conditions, verify that test fails
    cy.clickOptionWithText("Test Conditions");
    cy.get('[role="dialog"] [data-testid="search-input"]').type("hdfs");
    cy.waitTextVisible(datasetName);
    cy.clickOptionWithText("Run Test");
    cy.waitTextVisible("Failed");
    cy.clickOptionWithText("Close");
    // save edited test
    cy.get('[data-testid="modal-next-button"]').click();
    cy.clickOptionWithText("Save");
    cy.waitTextVisible("Successfully updated Test!");
    cy.waitTextVisible(testName);
    cy.reload();
    cy.waitTextVisible("No results found");
    // delete a test
    cy.visit("/tests");
    cy.get('[data-testid="test-more-button-0"]').click();
    cy.clickOptionWithText("Delete");
    cy.waitTextVisible("Confirm Test Removal");
    cy.clickOptionWithText("Yes");
    cy.waitTextVisible("Removed test.");
    cy.waitTextVisible("No tests found.");
    cy.ensureTextNotPresent(testName);
  });
});
