const test_id = crypto.getRandomValues(new Uint32Array(1))[0];
const username = `Example Name ${test_id}`;
const email = `example${test_id}@example.com`;
const password = "Example password";
const group_name = `Test group ${test_id}`;

const loginAndGoToDataset = () => {
  cy.loginWithCredentials();
  cy.goToDataset(
    "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)",
    "SampleCypressHiveDataset",
  );
};

const addOwner = (owner, type) => {
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
  cy.waitTextVisible(owner);
};

const verifyAssetInOwnedAssets = (owner) => {
  // ensure elastic is consistent before checking
  cy.wait(3000);
  cy.clickOptionWithText(owner);
  cy.waitTextVisible("SampleCypressHiveDataset");
};

const removeOwner = (owner, elementId) => {
  cy.goToDataset(
    "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)",
    "SampleCypressHiveDataset",
  );
  cy.get(elementId).next().click();
  cy.clickOptionWithText("Yes");
  cy.waitTextVisible("Owner Removed");
  cy.ensureTextNotPresent(owner);
};

const addAndRemoveOwnerOnDataset = (owner, type, elementId) => {
  loginAndGoToDataset();
  addOwner(owner, type);
  verifyAssetInOwnedAssets(owner);
  removeOwner(owner, elementId);
};

describe("add, remove ownership for dataset", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(false);
    cy.skipIntroducePage();
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

  it("open test dataset page, add and remove user ownership (Business Owner)", () => {
    addAndRemoveOwnerOnDataset(
      username,
      "Business Owner",
      `[href="/user/urn:li:corpuser:example${test_id}@example.com/owner of"]`,
    );
  });

  it("open test dataset page, add and remove user ownership (Data Steward)", () => {
    addAndRemoveOwnerOnDataset(
      username,
      "Data Steward",
      `[href="/user/urn:li:corpuser:example${test_id}@example.com/owner of"]`,
    );
  });

  it("open test dataset page, add and remove user ownership (None)", () => {
    addAndRemoveOwnerOnDataset(
      username,
      "None",
      `[href="/user/urn:li:corpuser:example${test_id}@example.com/owner of"]`,
    );
  });

  it("open test dataset page, add and remove user ownership (Technical Owner)", () => {
    addAndRemoveOwnerOnDataset(
      username,
      "Technical Owner",
      `[href="/user/urn:li:corpuser:example${test_id}@example.com/owner of"]`,
    );
  });

  it("open test dataset page, add and remove group ownership (Business Owner)", () => {
    addAndRemoveOwnerOnDataset(
      group_name,
      "Business Owner",
      `[href="/group/urn:li:corpGroup:${test_id}/owner of"]`,
    );
  });

  it("open test dataset page, add and remove group ownership (Data Steward)", () => {
    addAndRemoveOwnerOnDataset(
      group_name,
      "Data Steward",
      `[href="/group/urn:li:corpGroup:${test_id}/owner of"]`,
    );
  });

  it("open test dataset page, add and remove group ownership (None)", () => {
    addAndRemoveOwnerOnDataset(
      group_name,
      "None",
      `[href="/group/urn:li:corpGroup:${test_id}/owner of"]`,
    );
  });

  it("open test dataset page, add and remove group ownership (Technical Owner)", () => {
    addAndRemoveOwnerOnDataset(
      group_name,
      "Technical Owner",
      `[href="/group/urn:li:corpGroup:${test_id}/owner of"]`,
    );
  });
});
