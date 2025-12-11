/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import * as utils from "./utils";

const TEST_USER_URN = "urn:li:corpuser:jdoe";
const TEST_GLOSSARY_TERM_URN = "urn:li:glossaryTerm:CypressNode.CypressTerm";
const TEST_DOMAIN_NAME = "Testing";
const TEST_RELATED_TERM_NAME = "RelatedCypressTerm";

describe("summary tab - glossary term", () => {
  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
    cy.goToGlossaryTerm(TEST_GLOSSARY_TERM_URN);
    utils.goToSummaryTab();
  });

  it("glossary term - header section", () => {
    utils.testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", dataTestId: `owner-${TEST_USER_URN}` },
      { name: "Domain", type: "DOMAIN", value: TEST_DOMAIN_NAME },
    ]);
  });

  it.skip("glossary term - description section", () => {
    utils.testAboutSection();
  });

  it("glossary term - modules section", () => {
    const defaultModules = [
      {
        type: "assets",
        name: "Assets",
        value: TEST_DOMAIN_NAME,
      },
      {
        type: "related-terms",
        name: "Related Terms",
        value: TEST_RELATED_TERM_NAME,
      },
    ];

    utils.testTemplateSection(defaultModules);
  });
});
