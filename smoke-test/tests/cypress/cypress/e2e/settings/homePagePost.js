const homePageRedirection = () => {
    cy.visit('/')
    cy.waitTextPresent("Welcome back,")
}

const performTextAddAndEditAnnouncement = (text, title, discription, testId, updatedTitle) => {
    cy.waitTextPresent(text)
    cy.get('[data-testid="create-post-title"]').clear().type(title)
    cy.get('[id="description"]').clear().type(discription)
    cy.get(`[data-testid="${testId}-post-button"]`).click({ force: true });
    cy.reload()
    homePageRedirection();
    cy.waitTextPresent(updatedTitle);
}

const addAnnouncement = () => {
    cy.get('[id="posts-create-post"]').click({ force: true });
    performTextAddAndEditAnnouncement("Create new Post", "Test Announcement Title", "Add Description to post announcement", "create", "Test Announcement Title")
}

const editAnnouncement = () => {
    cy.get('[aria-label="more"]').first().click()
    cy.clickOptionWithText("Edit");
    performTextAddAndEditAnnouncement("Edit Post", "Test Announcement Title Edited", "Decription Edited", "update", "Test Announcement Title Edited")
}

const performTextAddAndEditLink = (text, title, url, imagesURL, testId, updatedTitle) => {
    cy.waitTextPresent(text)
    cy.get('[data-testid="create-post-title"]').clear().type(title)
    cy.get('[data-testid="create-post-link"]').clear().type(url)
    cy.get('[data-testid="create-post-media-location"]').clear().type(imagesURL)
    cy.get(`[data-testid="${testId}-post-button"]`).click({ force: true });
    cy.reload()
    homePageRedirection();
    cy.waitTextPresent(updatedTitle);
} 

const addLink = () => {
    cy.get('[id="posts-create-post"]').click({ force: true });
    cy.waitTextPresent('Create new Post')
    cy.contains('label', 'Link').click();
    performTextAddAndEditLink("Create new Post", "Test Link Title", 'https://www.example.com', 'https://www.example.com/images/example-image.jpg',"create", "Test Link Title")
}

const editLink = () => {
    cy.get('[aria-label="more"]').first().click()
    cy.clickOptionWithText("Edit");
    performTextAddAndEditLink("Edit Post", "Test Link Edited Title", 'https://www.updatedexample.com', 'https://www.updatedexample.com/images/example-image.jpg',"update", "Test Link Edited Title")
}  

const deleteFromPostDropdown = (post) => {
    cy.get('[aria-label="more"]').first().click()
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.reload()
    homePageRedirection();
    cy.ensureTextNotPresent(post);
}

describe("Create announcement and link posts", () => {
    beforeEach(() => {
        cy.loginWithCredentials();
        cy.goToHomePagePostSettings();
    });

    it("Create and Verify Announcement Post", () => {
        addAnnouncement();
    })

    it("Edit and verify Announcement Post", () => {
        editAnnouncement();
    })

    it("Delete and Verify Announcement Post", () => {
        deleteFromPostDropdown("Test Announcement Title Edited");
    })

    it("Create and Verify Link Post", () => {
        addLink()
    })

    it("Edit and Verify Link Post", () => {
        editLink()
    })

    it("Delete and Verify Link Post", () => {
        deleteFromPostDropdown("Test Link Edited Title");
    })
})