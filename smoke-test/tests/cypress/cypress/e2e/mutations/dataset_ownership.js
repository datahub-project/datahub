const test_id = crypto.getRandomValues(new Uint32Array(1))[0];
const username = `Example Name ${test_id}`;
const email = `example${test_id}@example.com`;
const password = "Example password";
const group_name = `Test group ${test_id}`;

const addOwner = (owner, type, elementId) => {
  cy.clickOptionWithTestId("add-owners-button");
  cy.contains("Search for users or groups...").click({ force: true });
  cy.focused().type(owner);
  cy.get(".ant-select-item").contains(owner).click();
  cy.focused().blur();
  cy.waitTextVisible(owner);
  cy.get('[role="dialog"]').contains("Technical Owner").click();
  cy.get('[role="listbox"]').parent().contains(type).click();
  cy.get('[role="dialog"]').contains(type).should("be.visible");
  cy.clickOptionWithText("Done");
  cy.waitTextVisible("Owners Added");
  cy.waitTextVisible(type);
  cy.waitTextVisible(owner).wait(3000);
  cy.clickOptionWithText(owner);
  cy.waitTextVisible("SampleCypressHiveDataset");
  cy.goToDataset(
    "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)",
    "SampleCypressHiveDataset",
  );
  cy.get(elementId).next().click();
  cy.clickOptionWithText("Yes");
  cy.waitTextVisible("Owner Removed");
  cy.ensureTextNotPresent(owner);
  cy.ensureTextNotPresent(type);
};

describe("add, remove ownership for dataset", () => {
  beforeEach(() => {
    cy.on("uncaught:exception", (err, runnable) => false);
  });

  it("create test user and test group, add user to a group", () => {
    cy.loginWithCredentials();
    cy.createUser(username, password, email);
    cy.createGroup(group_name, "Test group description", test_id);
    cy.addGroupMember(
      group_name,
      `/group/urn:li:corpGroup:${test_id}/assets`,
      username,
    );
  });

  it("open test dataset page, add and remove user ownership(test every type)", () => {
    cy.loginWithCredentials();
    cy.goToDataset(
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)",
      "SampleCypressHiveDataset",
    );
    // business owner
    addOwner(
      username,
      "Business Owner",
      `[href="/user/urn:li:corpuser:example${test_id}@example.com/owner of"]`,
    );
    // data steward
    addOwner(
      username,
      "Data Steward",
      `[href="/user/urn:li:corpuser:example${test_id}@example.com/owner of"]`,
    );
    // none
    addOwner(
      username,
      "None",
      `[href="/user/urn:li:corpuser:example${test_id}@example.com/owner of"]`,
    );
    // technical owner
    addOwner(
      username,
      "Technical Owner",
      `[href="/user/urn:li:corpuser:example${test_id}@example.com/owner of"]`,
    );
  });

  it("open test dataset page, add and remove group ownership(test every type)", () => {
    cy.loginWithCredentials();
    cy.goToDataset(
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)",
      "SampleCypressHiveDataset",
    );
    // business owner
    addOwner(
      group_name,
      "Business Owner",
      `[href="/group/urn:li:corpGroup:${test_id}/owner of"]`,
    );
    // data steward
    addOwner(
      group_name,
      "Data Steward",
      `[href="/group/urn:li:corpGroup:${test_id}/owner of"]`,
    );
    // none
    addOwner(
      group_name,
      "None",
      `[href="/group/urn:li:corpGroup:${test_id}/owner of"]`,
    );
    // technical owner
    addOwner(
      group_name,
      "Technical Owner",
      `[href="/group/urn:li:corpGroup:${test_id}/owner of"]`,
    );
  });
});
