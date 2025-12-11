/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export function createFile(content, fileName, fileType) {
  const blob = new Blob([content], { type: fileType });
  const file = new File([blob], fileName, { type: fileType });
  return file;
}

export function dropFile(file) {
  const dataTransfer = new DataTransfer();
  dataTransfer.items.add(file);

  cy.get(".remirror-editor").trigger("drop", {
    dataTransfer,
    force: true,
  });
}

export function ensureFileNode(urn, fileName, fileType) {
  cy.get(".remirror-editor").within(() => {
    cy.get(".file-node")
      .should("be.visible")
      .should(
        "have.attr",
        "data-file-url",
        `/openapi/v1/files/product_assets/${urn}`,
      )
      .should("contain", fileName);
  });
}

export function ensureErrorMessage(message, description) {
  cy.get(".ant-notification").should("be.visible");
  cy.get(".ant-notification").should("contain", message);
  if (description) {
    cy.get(".ant-notification").should("contain", description);
  }
}

export function clearDescription() {
  cy.clickOptionWithTestId("edit-description-button");
  cy.getWithTestId("description-editor").within(() => {
    cy.get(".remirror-editor").clear();
  });
  cy.clickOptionWithTestId("publish-button");
  cy.getWithTestId("publish-button").should("not.exist");
}
