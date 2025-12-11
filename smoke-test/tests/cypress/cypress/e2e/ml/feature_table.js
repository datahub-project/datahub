/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("features", () => {
  it("can visit feature tables and see features", () => {
    cy.visit("/");
    cy.login();
    cy.visit(
      "/featureTables/urn:li:mlFeatureTable:(urn:li:dataPlatform:sagemaker,cypress-feature-table)/Features?is_lineage_mode=false",
    );

    // the feature table descriptions should be there
    cy.contains("Yet another test feature group");
    cy.contains("this is a description from source system");

    // additional properties are visible
    cy.contains("CypressPrimaryKeyTag");
    cy.contains("CypressFeatureTag");

    // navigate to sources
    cy.contains("Sources").click();

    // feature & primary key sources are visible
    cy.contains("SampleCypressHdfsDataset");
    cy.contains("SampleCypressKafkaDataset");

    // navigate to properties
    cy.contains("Properties").click();

    // custom properties are visible
    cy.contains("status");
    cy.contains("Created");
  });

  it("can visit feature page", () => {
    cy.visit("/");
    cy.login();
    cy.visit(
      "/features/urn:li:mlFeature:(cypress-test-2,some-cypress-feature-1)/Feature%20Tables?is_lineage_mode=false",
    );

    // Shows the parent table
    cy.contains("cypress-feature-table");

    // Has upstream & downstream lineage
    cy.contains("1 upstream, 1 downstream");
  });

  it("can visit primary key page", () => {
    cy.visit("/");
    cy.login();
    cy.visit(
      "/mlPrimaryKeys/urn:li:mlPrimaryKey:(cypress-test-2,some-cypress-feature-2)/Feature%20Tables?is_lineage_mode=false",
    );

    // Shows the parent table
    cy.contains("cypress-feature-table");

    // Has upstream from its sources
    cy.contains("1 upstream, 0 downstream");
  });
});
