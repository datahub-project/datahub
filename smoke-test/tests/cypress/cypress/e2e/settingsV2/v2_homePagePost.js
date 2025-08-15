const homePageRedirection = () => {
  cy.visit("/");
  cy.get(`[data-testid="home-page-content-container"]`).should("be.visible");
};

const addOrEditAnnouncement = (text, title, description, testId) => {
  cy.waitTextPresent(text);
  cy.get('[data-testid="create-post-title"]').clear().type(title);
  // cy.get(".create-post-description").clear().type(description);
  cy.get(`[data-testid="${testId}-post-button"]`).click({ force: true });
  cy.reload();
  homePageRedirection();
};

const addOrEditLink = (text, title, url, imagesURL, testId) => {
  cy.waitTextPresent(text);
  cy.get('[data-testid="create-post-title"]').clear().type(title);
  cy.get('[data-testid="create-post-link"]').clear().type(url);
  cy.get('[data-testid="create-post-media-location"]').clear().type(imagesURL);
  cy.get(`[data-testid="${testId}-post-button"]`).click({ force: true });
  cy.reload();
  homePageRedirection();
};

const clickOnNewPost = () => {
  cy.get('[data-testid="posts-create-post-v2"]').click({ force: true });
};

const clickOnMoreOption = () => {
  cy.get('[aria-label="more"]').first().click();
};

describe("create announcement and link post", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.goToHomePagePostSettingsV2();
    cy.waitTestIdVisible("posts-create-post-v2");
  });

  it("Verify create, edit and delete announcement post", () => {
    clickOnNewPost();
    addOrEditAnnouncement(
      "Create",
      "Test Announcement Title",
      "Add Description to post announcement",
      "create",
    );
    cy.clickOptionWithId("#v2-home-page-announcements");
    cy.waitTextPresent("Test Announcement Title");
    cy.goToHomePagePostSettingsV2();
    clickOnMoreOption();
    cy.clickOptionWithText("Edit");
    cy.contains("label", "Announcement").click();
    addOrEditAnnouncement(
      "Edit",
      "Test Announcement Title Updated",
      "Decription Updated",
      "update",
    );
    cy.clickOptionWithId("#v2-home-page-announcements");
    cy.waitTextPresent("Test Announcement Title Updated");
    cy.goToHomePagePostSettingsV2();
    clickOnMoreOption();
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    homePageRedirection();
    cy.wait(500);
    cy.reload();
    cy.ensureTextNotPresent("Test Announcement Title Updated");
  });

  it("Verify create, edit and delete link post", () => {
    clickOnNewPost();
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
    cy.goToHomePagePostSettingsV2();
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
    cy.goToHomePagePostSettingsV2();
    clickOnMoreOption();
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    homePageRedirection();
    cy.wait(500);
    cy.reload();
    cy.ensureTextNotPresent("Test Link Updated Title");
  });
});
