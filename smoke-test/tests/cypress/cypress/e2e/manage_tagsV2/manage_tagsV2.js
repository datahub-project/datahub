import DatasetHelper from "./helpers/dataset_helper";
import TagsPageHelper from "./helpers/tags_page_helper";

const test_id = `manage_tagsV2_${new Date().getTime()}`;

const SAMPLE_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)";
const SAMPLE_DATASET_NAME = "SampleCypressHiveDataset";

describe("tags", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.loginWithCredentials();
  });

  it("should allow to create/edit/remove tags on tags page", () => {
    const tagName = `tag_${test_id}_tags_page`;
    const tagDescription = `${tagName} description`;

    TagsPageHelper.openPage();
    TagsPageHelper.create(tagName, tagDescription);
    TagsPageHelper.ensureTagIsInTable(tagName, tagDescription);
    // ensure that we can't to create tag with the same name
    TagsPageHelper.create(tagName, tagDescription, false);
    TagsPageHelper.edit(tagName, `${tagDescription} edited`);
    TagsPageHelper.ensureTagIsInTable(tagName, `${tagDescription} edited`);
    TagsPageHelper.remove(tagName);
    TagsPageHelper.ensureTagIsNotInTable(tagName);
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
