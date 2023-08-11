const test_id = Math.floor(Math.random() * 100000);
const documentation = `This is test${test_id} documentation`;
const documentation_edited = `This is test${test_id} documentation EDITED`;
const wrong_url = "https://datahubio";
const correct_url = "https://datahub.io";

describe("add, remove documentation and link to dataset", () => {

    it("open test dataset page, add and remove dataset documentation", () => {
        //add documentation, and propose, decline proposal
        cy.loginWithCredentials();
        cy.visit("/dataset/urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressDocumentationDataset,PROD)/Schema");
        cy.waitTextVisible("No documentation yet. Share your knowledge by adding documentation and links to helpful resources.");
        cy.waitTextVisible("Add Documentation")
        cy.get("[role='tab']").contains("Documentation").click();
        cy.waitTextVisible("No documentation yet")
        cy.waitTextVisible("Share your knowledge by adding documentation and links to helpful resources.")
        cy.clickOptionWithText("Add Documentation");
        cy.focused().type(documentation);
        cy.get("button").contains("Propose").click();
        cy.waitTextVisible("Proposed description update!").wait(3000);
        cy.waitTextVisible("No documentation yet");
        cy.clickOptionWithText("Inbox");
        cy.waitTextVisible("Update Description Proposal");
        cy.contains("View difference").first().click();
        cy.waitTextVisible(documentation);
        cy.get("[data-icon='close'").click();
        cy.get("button").contains("Decline").click();
        cy.waitTextVisible("Are you sure you want to reject this proposal?");
        cy.get("button").contains("Yes").click();
        cy.waitTextVisible("Proposal declined.");
        cy.visit("/dataset/urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressDocumentationDataset,PROD)/Schema");
        cy.get("[role='tab']").contains("Documentation").click();
        cy.waitTextVisible("No documentation yet")
        //add documentation, and propose, approve proposal
        cy.clickOptionWithText("Add Documentation");
        cy.focused().type(documentation);
        cy.get("button").contains("Propose").click();
        cy.waitTextVisible("Proposed description update!").wait(3000);
        cy.waitTextVisible("No documentation yet");
        cy.clickOptionWithText("Inbox");
        cy.waitTextVisible("Update Description Proposal");
        cy.get("button").contains("Approve").click();
        cy.waitTextVisible("Are you sure you want to accept this proposal?");
        cy.get("button").contains("Yes").click();
        cy.waitTextVisible("Successfully accepted the proposal!");
        cy.visit("/dataset/urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressDocumentationDataset,PROD)/Schema");
        cy.get("[role='tab']").contains("Documentation").click();
        cy.ensureTextNotPresent("No documentation yet");
        cy.waitTextVisible(documentation);
        cy.ensureTextNotPresent("Add Documentation");
        //edit and save documentation
        cy.clickOptionWithTestId("edit-description");
        cy.focused().clear().type(documentation_edited);
        cy.clickOptionWithTestId("save-description");
        cy.waitTextVisible("Description Updated");
        cy.waitTextVisible(documentation_edited);
        //remove documentation
        cy.clickOptionWithTestId("edit-description");
        cy.focused().clear();
        cy.clickOptionWithTestId("save-description");
        cy.waitTextVisible("Description Updated");
        cy.ensureTextNotPresent(documentation_edited);
    });

    it("open test dataset page, add and remove dataset link", () => {
        cy.loginWithCredentials();
        cy.visit("/dataset/urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressDocumentationDataset,PROD)/Schema");
        cy.get("button").contains("Add Link").click();
        cy.get("#addLinkForm_url").type(wrong_url);
        cy.waitTextVisible("This field must be a valid url.");
        cy.focused().clear()
        cy.waitTextVisible("A URL is required.");
        cy.focused().type(correct_url);
        cy.ensureTextNotPresent("This field must be a valid url.");
        cy.get("#addLinkForm_label").type(test_id);
        cy.get('[role="dialog"] button').contains("Add").click();
        cy.waitTextVisible("Link Added");
        cy.get("[role='tab']").contains("Documentation").click();
        cy.get(`[href='${correct_url}']`).should("be.visible");
        cy.get(`[href='${correct_url}']`).eq(1).trigger("mouseover", { force: true });
        cy.get('[data-icon="delete"]').click();
        cy.waitTextVisible("Link Removed");
        cy.get(`[href='${correct_url}']`).should("not.exist");
        cy.waitTextVisible("Add Documentation")
        cy.waitTextVisible("No documentation yet")
    });
});