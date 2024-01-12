const announcementTitle = 'Test Announcement Title'
const linkTitle = 'Test Link Title'
const description = 'Add Description to post announcement'
const linkURL = 'https://www.example.com'
const imagesURL = 'https://www.example.com/images/example-image.jpg'

const homePageRedirection = () => {
    cy.visit('/')
    cy.waitTextPresent("Welcome back,")
}

const addAnnouncement = (title, description) => {
    cy.get('[id="posts-create-post"]').click({ force: true });
    cy.enterTextInTestId("create-post-title", title);
    cy.get('[id="description"]').type(description)
    cy.get('[data-testid="create-post-button"]').click({ force: true });
    homePageRedirection();
    cy.waitTextPresent(announcementTitle);
}

const addLink = (linkTitle, linkURL, imageURL) => {
    cy.get('[id="posts-create-post"]').click({ force: true });
    cy.clickOptionWithText('Link');
    cy.enterTextInTestId('create-post-title', linkTitle);
    cy.enterTextInTestId('create-post-link', linkURL);
    cy.enterTextInTestId('create-post-media-location', imageURL)
    cy.get('[data-testid="create-post-button"]').click({ force: true });
    homePageRedirection();
    cy.waitTextPresent(linkTitle)
}

const deleteFromPostDropdown = (text) => {
    cy.get('[aria-label="more"]').first().click()
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    homePageRedirection();
    cy.ensureTextNotPresent(text)
}

describe("Create announcement and link posts", () => {
    beforeEach(() => {
        cy.loginWithCredentials();
        cy.goToHomePagePostSettings();
    });

    it("Create and Verify Announcement Post", () => {
        addAnnouncement(announcementTitle, description);
    })

    it("Delete and Verify Announcement Post", () => {
        deleteFromPostDropdown(announcementTitle);
    })

    it("Create and Verify Link Post", () => {
        addLink(linkTitle, linkURL, imagesURL)
    })

    it("Delete and Verify Link Post", () => {
        deleteFromPostDropdown(linkTitle);
    })
})