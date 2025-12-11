/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("models", () => {
  it("can visit sagemaker models and groups", () => {
    cy.visitWithLogin(
      "/mlModels/urn:li:mlModel:(urn:li:dataPlatform:sagemaker,cypress-model,PROD)/Summary?is_lineage_mode=false",
    );

    cy.contains("ml model description");

    // the model has metrics & hyper params
    cy.contains("another-metric");
    cy.contains("parameter-1");

    // the model has features
    cy.contains("Features").click();
    cy.contains("some-cypress-feature-1");

    // the model has a group
    cy.visit(
      "/mlModels/urn:li:mlModel:(urn:li:dataPlatform:sagemaker,cypress-model,PROD)/Group?is_lineage_mode=false",
    );
    cy.contains("cypress-model-package-group");
  });

  it("can visit sagemaker models and groups", () => {
    cy.visitWithLogin(
      "/mlModelGroup/urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,cypress-model-package-group,PROD)",
    );
    // the model group has its model
    cy.contains("cypress-model");
    cy.contains("Just a model package group.");
  });
});
