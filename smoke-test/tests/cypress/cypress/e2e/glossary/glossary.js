/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

const urn =
  "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
const datasetName = "cypress_logging_events";
const glossaryTerm = "CypressGlossaryTerm";
const glossaryTermGroup = "CypressGlossaryGroup";

describe("glossary", () => {
  it("go to glossary page, create terms, term group", () => {
    cy.loginWithCredentials();
    cy.goToGlossaryList();
    cy.clickOptionWithText("Add Term");
    cy.addViaModal(
      glossaryTerm,
      "Create Glossary Term",
      glossaryTerm,
      "glossary-entity-modal-create-button",
    );
    cy.clickOptionWithText("Add Term Group");
    cy.addViaModal(
      glossaryTermGroup,
      "Create Term Group",
      glossaryTermGroup,
      "glossary-entity-modal-create-button",
    );
    cy.addTermToDataset(urn, datasetName, glossaryTerm);
    cy.waitTextVisible(glossaryTerm);
    cy.goToGlossaryList();
    cy.clickOptionWithText(glossaryTerm);
    cy.deleteFromDropdown();
    cy.goToDataset(urn, datasetName);
    cy.ensureTextNotPresent(glossaryTerm);
    cy.goToGlossaryList();
    cy.clickOptionWithText(glossaryTermGroup);
    cy.deleteFromDropdown();
    cy.goToGlossaryList();
    cy.ensureTextNotPresent(glossaryTermGroup);
  });
});
