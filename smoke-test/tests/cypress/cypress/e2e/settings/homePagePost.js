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
  cy.get('[id="posts-create-post"]').click({ force: true });
};

const clickOnMoreOption = () => {
  cy.get('[aria-label="more"]').first().click();
};

describe("create announcement and link post", () => {
    beforeEach(() => {
        cy.loginWithCredentials();
        cy.goToHomePagePostSettings();
        cy.waitTextPresent('Platform')
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
    cy.contains('label', 'Announcement').click();
        addOrEditAnnouncement("Edit Post", "Test Announcement Title Updated",
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
    homePageRedirection();
    cy.ensureTextNotPresent("Test Announcement Title Updated");
  });

  it("create link post and verify", () => {
    clickOnNewPost('Link');
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
    cy.contains('label', 'Link').click();
        addOrEditLink("Edit Post", "Test Link Updated Title",
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
    homePageRedirection();
    cy.ensureTextNotPresent("Test Link Updated Title");
  });
});
