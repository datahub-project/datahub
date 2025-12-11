/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import DatasetHelper from "./helpers/dataset_helper";
import TagsPageHelper from "./helpers/tags_page_helper";

const test_id = `manage_tagsV2_${new Date().getTime()}`;

const SAMPLE_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)";
const SAMPLE_DATASET_NAME = "SampleCypressHiveDataset";

describe("tags - assign/unassign", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  it("should allow to assign/unassign tags on a dataset", () => {
    const tagName = `tag_${test_id}_dataset`;
    const tagDescription = `${tagName} description`;

    TagsPageHelper.openPage();
    TagsPageHelper.create(tagName, tagDescription);

    DatasetHelper.openDataset(SAMPLE_DATASET_URN, SAMPLE_DATASET_NAME);
    DatasetHelper.assignTag(tagName);
    DatasetHelper.ensureTagIsAssigned(tagName);

    DatasetHelper.searchByTag(tagName);
    DatasetHelper.ensureEntityIsInSearchResults(SAMPLE_DATASET_URN);

    DatasetHelper.openDataset(SAMPLE_DATASET_URN, SAMPLE_DATASET_NAME);
    DatasetHelper.unassignTag(tagName);
    DatasetHelper.ensureTagIsNotAssigned(tagName);

    DatasetHelper.searchByTag(tagName);
    DatasetHelper.ensureEntityIsNotInSearchResults(SAMPLE_DATASET_URN);

    TagsPageHelper.openPage();
    TagsPageHelper.remove(tagName);
  });
});
