const title = 'Test Link Title'
const url = 'https://www.example.com'
const imagesURL = 'https://www.example.com/images/example-image.jpg'

const homePageRedirection = () => {
    cy.visit('/')
    cy.waitTextPresent("Welcome back,")
}

const addAnnouncement = () => {
    cy.get('[id="posts-create-post"]').click({ force: true });
    cy.waitTextPresent('Create new Post')
    cy.enterTextInTestId("create-post-title", "Test Announcement Title");
    cy.get('[id="description"]').type("Add Description to post announcement")
    cy.get('[data-testid="create-post-button"]').click({ force: true });
    cy.reload()
    homePageRedirection();
    cy.waitTextPresent("Test Announcement Title");
}

const addLink = (title,url,imagesURL) => {
    cy.get('[id="posts-create-post"]').click({ force: true });
    cy.waitTextPresent('Create new Post')
    cy.clickOptionWithText('Link');
    cy.enterTextInTestId('create-post-title', title);
    cy.enterTextInTestId('create-post-link', url);
    cy.enterTextInTestId('create-post-media-location', imagesURL)
    cy.get('[data-testid="create-post-button"]').click({ force: true });
    cy.reload()
    homePageRedirection();
    cy.waitTextPresent(title)
}

const deleteFromPostDropdown = () => {
    cy.get('[aria-label="more"]').first().click()
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.reload()
    homePageRedirection();
}

describe("Create announcement and link posts", () => {
    beforeEach(() => {
        cy.loginWithCredentials();
        cy.goToHomePagePostSettings();
    });

    it("Create and Verify Announcement Post", () => {
        addAnnouncement();
    })

    it("Delete and Verify Announcement Post", () => {
        deleteFromPostDropdown();
        cy.ensureTextNotPresent("Test Announcement Title")
    })

    it("Create and Verify Link Post", () => {
        addLink(title,url,imagesURL)
    })

    it("Delete and Verify Link Post", () => {
        deleteFromPostDropdown();
        cy.ensureTextNotPresent(title);
    })
})