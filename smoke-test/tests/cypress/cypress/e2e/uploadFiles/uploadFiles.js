/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import {
  applyGraphqlInterceptors,
  getThemeV2Interceptor,
  getUniqueTestId,
  hasOperationName,
} from "../utils";
import {
  clearDescription,
  createFile,
  dropFile,
  ensureErrorMessage,
  ensureFileNode,
} from "./utils";

function getSetRequiredFeatureFlagsInterceptor() {
  return (req, res) => {
    if (hasOperationName(req, "appConfig")) {
      res.body.data.appConfig.featureFlags.documentationFileUploadV1 = true;
      res.body.data.appConfig.featureFlags.assetSummaryPageV1 = true;
    }
  };
}

// const testId = getUniqueTestId();

describe("uploadFiles", () => {
  const setupInterceptors = (testId) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "getPresignedUploadUrl")) {
        req.reply({
          body: {
            data: {
              getPresignedUploadUrl: {
                url: `${Cypress.config().baseUrl}presigned_url`,
                fileId: `urn:li:dataHubFile:test_${testId}`,
              },
            },
          },
        });
      }
    });

    cy.intercept("PUT", "/presigned_url", (req) => {
      req.reply({ statusCode: 200 });
    });
  };

  beforeEach(() => {
    applyGraphqlInterceptors([
      getThemeV2Interceptor(true),
      getSetRequiredFeatureFlagsInterceptor(),
    ]);

    cy.login();
    cy.skipIntroducePage();
    cy.goToDomain("urn:li:domain:marketing");
  });

  it("should allow to drop file", () => {
    const testId = getUniqueTestId();
    setupInterceptors(testId);
    const file = createFile("content", "test.txt", "text/plain");

    cy.clickOptionWithTestId("edit-description-button");

    cy.getWithTestId("description-editor").within(() => {
      dropFile(file);

      ensureFileNode(
        `urn:li:dataHubFile:test_${testId}`,
        "test.txt",
        "text/plain",
      );
    });
  });

  it("should allow to upload file by button", () => {
    const testId = getUniqueTestId();
    setupInterceptors(testId);
    const file = createFile("content", "test.txt", "text/plain");

    cy.clickOptionWithTestId("edit-description-button");

    cy.getWithTestId("description-editor").within(() => {
      cy.clickOptionWithTestId("command-uploadFile-btn");
    });

    cy.getWithTestId("file-upload-input").selectFile(
      {
        contents: file,
        fileName: "test.txt",
      },
      { force: true },
    );

    cy.getWithTestId("description-editor").within(() => {
      ensureFileNode(
        `urn:li:dataHubFile:test_${testId}`,
        "test.txt",
        "text/plain",
      );
    });
  });

  it("should render file in readonly mode", () => {
    const testId = getUniqueTestId();
    setupInterceptors(testId);
    const file = createFile("content", "test.txt", "text/plain");

    cy.clickOptionWithTestId("edit-description-button");

    cy.getWithTestId("description-editor").within(() => {
      dropFile(file);
      ensureFileNode(
        `urn:li:dataHubFile:test_${testId}`,
        "test.txt",
        "text/plain",
      );
    });

    cy.clickOptionWithTestId("publish-button");

    cy.getWithTestId("about-section").within(() => {
      ensureFileNode(`urn:li:dataHubFile:test_${testId}`, "test.txt");
    });

    clearDescription();
  });

  it("should validate file type", () => {
    const testId = getUniqueTestId();
    setupInterceptors(testId);
    const file = createFile("content", "test.unknown", "unknown");

    cy.clickOptionWithTestId("edit-description-button");

    cy.getWithTestId("description-editor").within(() => {
      dropFile(file);
    });

    ensureErrorMessage("Upload Failed", "File type not supported: UNKNOWN");
  });
});
