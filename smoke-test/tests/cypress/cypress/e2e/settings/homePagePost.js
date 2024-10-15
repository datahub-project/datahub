const homePageRedirection = () => {
  cy.visit("/");
  cy.waitTextPresent("Welcome back");
};

const addOrEditAnnouncement = (text, title, description, testId) => {
  cy.waitTextPresent(text);
  cy.get('[data-testid="create-post-title"]').clear().type(title);
  cy.get(".create-post-description").clear().type(description);
  cy.get(`[data-testid="${testId}-post-button"]`).click({ force: true });
  cy.get(".ant-table-row ").contains(title).should("be.visible");
  cy.reload();
  homePageRedirection();
};

const addOrEditLink = (text, title, url, imagesURL, testId) => {
  cy.waitTextPresent(text);
  cy.get('[data-testid="create-post-title"]').clear().type(title);
  cy.get('[data-testid="create-post-link"]').clear().type(url);
  cy.get('[data-testid="create-post-media-location"]').clear().type(imagesURL);
  cy.get(`[data-testid="${testId}-post-button"]`).click({ force: true });
  cy.get(".ant-table-row ").contains(title).should("be.visible");
  cy.reload();
  homePageRedirection();
};

const clickOnNewPost = () => {
  cy.get('[data-testid="posts-create-post"]').click({ force: true });
};

const clickOnMoreOption = () => {
  cy.get('[aria-label="more"]').first().click();
};

describe("create announcement and link post", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(false);
    cy.loginWithCredentials();
    cy.goToHomePagePostSettingsV1();
    cy.waitTestIdVisible("posts-create-post");
  });

  it("create announcement post and verify", () => {
    clickOnNewPost();
    addOrEditAnnouncement(
      "Create",
      "Test Announcement Title",
      "Add Description to post announcement",
      "create",
    );
    cy.waitTextPresent("Test Announcement Title");
  });

  it("edit announced post and verify", () => {
    clickOnMoreOption();
    cy.clickOptionWithText("Edit");
    cy.contains("label", "Announcement").click();
    addOrEditAnnouncement(
      "Edit",
      "Test Announcement Title Updated",
      "Decription Updated",
      "update",
    );
    cy.waitTextPresent("Test Announcement Title Updated");
  });

  it("delete announced post and verify", () => {
    clickOnMoreOption();
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.reload();
    cy.ensureTextNotPresent("Test Announcement Title Updated");
    homePageRedirection();
    cy.ensureTextNotPresent("Test Announcement Title Updated");
  });

  it("create link post and verify", () => {
    clickOnNewPost("Link");
    cy.waitTextPresent("Create");
    cy.contains("label", "Pinned Link").click();
    addOrEditLink(
      "Create",
      "Test Link Title",
      "https://www.example.com",
      "https://www.example.com/images/example-image.jpg",
      "create",
    );
    cy.waitTextPresent("Test Link Title");
  });

  it("edit linked post and verify", () => {
    clickOnMoreOption();
    cy.clickOptionWithText("Edit");
    cy.contains("label", "Pinned Link").click();
    addOrEditLink(
      "Edit",
      "Test Link Updated Title",
      "https://www.updatedexample.com",
      "https://www.updatedexample.com/images/example-image.jpg",
      "update",
    );
    cy.waitTextPresent("Test Link Updated Title");
  });

  it("delete linked post and verify", () => {
    clickOnMoreOption();
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.reload();
    cy.ensureTextNotPresent("Test Link Updated Title");
    homePageRedirection();
    cy.ensureTextNotPresent("Test Link Updated Title");
  });
});
