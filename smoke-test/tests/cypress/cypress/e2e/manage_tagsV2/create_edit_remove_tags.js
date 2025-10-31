import TagsPageHelper from "./helpers/tags_page_helper";

const test_id = `manage_tagsV2_${new Date().getTime()}`;

describe("tags - create/edit/remove", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
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
});
