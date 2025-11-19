import { applyGraphqlInterceptors, getThemeV2Interceptor } from "../utils";
import { SearchV2Helper } from "./helpers/searchV2Helper";

const SAMPLE_ENTITY_NAME = "SampleCypressKafkaDataset";
const SAMPLE_ENTITY_URN =
  "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)";

describe("searchBarV2 - Filter Application", () => {
  const helper = new SearchV2Helper();

  const setupTest = (searchBarApi = "SEARCH_ACROSS_ENTITIES") => {
    applyGraphqlInterceptors([
      getThemeV2Interceptor(true),
      helper.getSearchBarInterceptor(true, searchBarApi),
    ]);

    cy.login();
    cy.skipIntroducePage();
  };

  it("should apply filters correctly", () => {
    setupTest();
    helper.goToHomePage();

    helper.searchBarV2.type(SAMPLE_ENTITY_NAME);

    // Check platform filter
    helper.searchBarV2.filters.apply("platform", ["Kafka"]);
    helper.searchBarV2.filters.ensureValuesSelected("platform", ["Kafka"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);
    helper.searchBarV2.filters.clear("platform");
    helper.searchBarV2.filters.ensureValuesNotSelected("platform", ["Kafka"]);
    helper.searchBarV2.filters.apply("platform", ["Looker"]);
    helper.searchBarV2.filters.ensureValuesSelected("platform", ["Looker"]);
    helper.searchBarV2.ensureEntityNotInResponse(SAMPLE_ENTITY_URN);
    helper.searchBarV2.filters.clear("platform");
    helper.searchBarV2.filters.ensureValuesNotSelected("platform", ["Looker"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);

    // Check entity type filter
    helper.searchBarV2.filters.apply("entity-type", ["Dataset"]);
    helper.searchBarV2.filters.ensureValuesSelected("entity-type", ["Dataset"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);
    helper.searchBarV2.filters.clear("entity-type");
    helper.searchBarV2.ensureEntityNotInResponse("entity-type", ["Dataset"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);

    // Check owner filter
    helper.searchBarV2.filters.apply("owner", ["DataHub"]);
    helper.searchBarV2.filters.ensureValuesSelected("owner", ["DataHub"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);
    helper.searchBarV2.filters.clear("owner");
    helper.searchBarV2.ensureEntityNotInResponse("owner", ["DataHub"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);

    // Check tag filter
    helper.searchBarV2.filters.apply("tag", ["Cypress"]);
    helper.searchBarV2.filters.ensureValuesSelected("tag", ["Cypress"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);
    helper.searchBarV2.filters.clear("tag");
    helper.searchBarV2.filters.ensureValuesNotSelected("tag", ["Cypress"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);
    helper.searchBarV2.filters.apply("tag", ["Cypress 2"]);
    helper.searchBarV2.filters.ensureValuesSelected("tag", ["Cypress 2"]);
    helper.searchBarV2.ensureEntityNotInResponse(SAMPLE_ENTITY_URN);
    helper.searchBarV2.filters.clear("tag");
    helper.searchBarV2.filters.ensureValuesNotSelected("tag", ["Cypress 2"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);

    // Check domain filter
    helper.searchBarV2.filters.apply("domain", ["Marketing"]);
    helper.searchBarV2.filters.ensureValuesSelected("domain", ["Marketing"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);
    helper.searchBarV2.filters.clear("domain");
    helper.searchBarV2.filters.ensureValuesNotSelected("domain", ["Marketing"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);
    helper.searchBarV2.filters.apply("domain", ["Testing"]);
    helper.searchBarV2.filters.ensureValuesSelected("domain", ["Testing"]);
    helper.searchBarV2.ensureEntityNotInResponse(SAMPLE_ENTITY_URN);
    helper.searchBarV2.filters.clear("domain");
    helper.searchBarV2.filters.ensureValuesNotSelected("domain", ["Testing"]);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);
  });
});
