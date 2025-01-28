const homePageRedirection = () => {
  cy.visit("/");
  cy.waitTextPresent("Welcome back");
};

const addOrEditAnnouncement = (text, title, description, testId) => {
  cy.waitTextPresent(text);
  cy.get('[data-testid="create-post-title"]').clear().type(title);
  cy.get('[id="description"]').clear().type(description);
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
  cy.get('[data-testid="posts-create-post"]').click({ force: true });
};

const clickOnMoreOption = () => {
  cy.get('[aria-label="more"]').first().click();
};

describe("create announcement and link post", () => {
  beforeEach(() => {
    cy.loginWithCredentials();
    cy.goToHomePagePostSettings();
  });

  it("create announcement post and verify", () => {
    clickOnNewPost();
    addOrEditAnnouncement(
      "Create new Post",
      "Test Announcement Title",
      "Add Description to post announcement",
      "create",
    );
    cy.waitTextPresent("Test Announcement Title");
  });

  it("edit announced post and verify", () => {
    clickOnMoreOption();
    cy.clickOptionWithText("Edit");
    addOrEditAnnouncement(
      "Edit Post",
      "Test Announcement Title Edited",
      "Decription Edited",
      "update",
    );
    cy.waitTextPresent("Test Announcement Title Edited");
  });

  it("delete announced post and verify", () => {
    clickOnMoreOption();
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.reload();
    homePageRedirection();
    cy.ensureTextNotPresent("Test Announcement Title Edited");
  });

  it("create link post and verify", () => {
    clickOnNewPost();
    cy.waitTextPresent("Create new Post");
    cy.contains("label", "Link").click();
    addOrEditLink(
      "Create new Post",
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
    addOrEditLink(
      "Edit Post",
      "Test Link Edited Title",
      "https://www.updatedexample.com",
      "https://www.updatedexample.com/images/example-image.jpg",
      "update",
    );
    cy.waitTextPresent("Test Link Edited Title");
  });

  it("delete linked post and verify", () => {
    clickOnMoreOption();
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.reload();
    homePageRedirection();
    cy.ensureTextNotPresent("Test Link Edited Title");
  });
});
