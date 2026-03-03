const TEST_ANNOUNCEMENT_TITLE = "Test Announcement Title";
const EDITED_TEST_ANNOUNCEMENT_TITLE = "Test Announcement Title";
const TEST_LINK_TITLE = "Test Link Title";
const EDITED_TEST_LINK_TITLE = "Test Link Updated Title";

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

const clickOnMoreOption = (title) => {
  cy.get(`[data-testid="dropdown-menu-${title}"]`).click({ force: true });
};

describe("create announcement and link post", () => {
  beforeEach(() => {
    cy.loginWithCredentials();
    cy.goToHomePagePostSettings();
  });

  it("create announcement post and verify", () => {
    clickOnNewPost();
    addOrEditAnnouncement(
      "Create",
      TEST_ANNOUNCEMENT_TITLE,
      "Add Description to post announcement",
      "create",
    );
    cy.waitTextPresent(TEST_ANNOUNCEMENT_TITLE);
  });

  it("edit announced post and verify", () => {
    clickOnMoreOption(TEST_ANNOUNCEMENT_TITLE);
    cy.clickOptionWithText("Edit");
    addOrEditAnnouncement(
      "Edit",
      EDITED_TEST_ANNOUNCEMENT_TITLE,
      "Decription Updated",
      "update",
    );
    cy.waitTextPresent(EDITED_TEST_ANNOUNCEMENT_TITLE);
  });

  it("delete announced post and verify", () => {
    clickOnMoreOption(EDITED_TEST_ANNOUNCEMENT_TITLE);
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.reload();
    cy.ensureTextNotPresent(EDITED_TEST_ANNOUNCEMENT_TITLE);
    homePageRedirection();
    cy.ensureTextNotPresent(EDITED_TEST_ANNOUNCEMENT_TITLE);
  });

  it("create link post and verify", () => {
    clickOnNewPost();
    cy.waitTextPresent("Create new Post");
    cy.contains("label", "Link").click();
    addOrEditLink(
      "Create",
      TEST_LINK_TITLE,
      "https://www.example.com",
      "https://www.example.com/images/example-image.jpg",
      "create",
    );
    cy.waitTextPresent(TEST_LINK_TITLE);
  });

  it("edit linked post and verify", () => {
    clickOnMoreOption(TEST_LINK_TITLE);
    cy.clickOptionWithText("Edit");
    addOrEditLink(
      "Edit",
      EDITED_TEST_LINK_TITLE,
      "https://www.updatedexample.com",
      "https://www.updatedexample.com/images/example-image.jpg",
      "update",
    );
    cy.waitTextPresent(EDITED_TEST_LINK_TITLE);
  });

  it("delete linked post and verify", () => {
    clickOnMoreOption(EDITED_TEST_LINK_TITLE);
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.reload();
    cy.ensureTextNotPresent(EDITED_TEST_LINK_TITLE);
    homePageRedirection();
    cy.ensureTextNotPresent(EDITED_TEST_LINK_TITLE);
  });
});
