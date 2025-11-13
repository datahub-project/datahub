import { applyGraphqlInterceptors, getThemeV2Interceptor } from "../utils";
import { SearchV2Helper } from "./helpers/searchV2Helper";

const SAMPLE_ENTITY_NAME = "SampleCypressKafkaDataset";
const SAMPLE_ENTITY_URN =
  "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)";

describe("searchBarV2 - Matched Fields", () => {
  const helper = new SearchV2Helper();

  const setupTest = (searchBarApi = "SEARCH_ACROSS_ENTITIES") => {
    applyGraphqlInterceptors([
      getThemeV2Interceptor(true),
      helper.getSearchBarInterceptor(true, searchBarApi),
    ]);

    cy.login();
    cy.skipIntroducePage();
  };

  it("should show matched fields when possible", () => {
    setupTest("SEARCH_ACROSS_ENTITIES");
    helper.goToHomePage();

    helper.searchBarV2.type("field_foo_2");
    helper.searchBarV2.filters.apply("platform", ["Kafka"]);
    helper.searchBarV2.filters.apply("tag", ["Cypress"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);
    helper.searchBarV2.ensureMatchedFieldHasText(
      SAMPLE_ENTITY_URN,
      "fieldPaths",
      "field_foo_2",
    );
  });
});