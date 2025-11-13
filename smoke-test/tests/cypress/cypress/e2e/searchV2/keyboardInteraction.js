import { applyGraphqlInterceptors, getThemeV2Interceptor } from "../utils";
import { SearchV2Helper } from "./helpers/searchV2Helper";

const SAMPLE_ENTITY_NAME = "SampleCypressKafkaDataset";
const SAMPLE_ENTITY_URN =
  "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)";

describe("searchBarV2 - Keyboard Interaction", () => {
  const helper = new SearchV2Helper();

  const setupTest = (searchBarApi = "SEARCH_ACROSS_ENTITIES") => {
    applyGraphqlInterceptors([
      getThemeV2Interceptor(true),
      helper.getSearchBarInterceptor(true, searchBarApi),
    ]);

    cy.login();
    cy.skipIntroducePage();
  };

  it("should handle keyboard", () => {
    setupTest();
    helper.goToHomePage();

    // Opening by shortcut
    helper.searchBarV2.openByShortcut();
    helper.searchBarV2.ensureFocused();
    helper.searchBarV2.ensureOpened();

    // Fill the search bar and apply filter
    helper.searchBarV2.type(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.ensureTextInSearchBar(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.ensureOpened();
    helper.searchBarV2.filters.apply("platform", ["Kafka"]);

    // The first Esc just closes the search bars dropdown
    helper.searchBarV2.pressEscape();
    helper.searchBarV2.ensureTextInSearchBar(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.ensureClosed();

    // The second Esc cleans query and applied filters in the search bar
    helper.searchBarV2.pressEscape();
    helper.searchBarV2.ensureTextInSearchBar("");
    helper.searchBarV2.ensureClosed();

    // check if platform filter is empty
    helper.searchBarV2.openByShortcut();
    helper.searchBarV2.type(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.filters.ensureValuesNotSelected("platform", ["Kafka"]);
  });
});