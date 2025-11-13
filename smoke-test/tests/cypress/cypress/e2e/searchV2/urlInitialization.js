import { applyGraphqlInterceptors, getThemeV2Interceptor } from "../utils";
import { SearchV2Helper } from "./helpers/searchV2Helper";

const SAMPLE_ENTITY_NAME = "SampleCypressKafkaDataset";
const SAMPLE_ENTITY_URN =
  "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)";

describe("searchBarV2 - URL Initialization", () => {
  const helper = new SearchV2Helper();

  const setupTest = (searchBarApi = "SEARCH_ACROSS_ENTITIES") => {
    applyGraphqlInterceptors([
      getThemeV2Interceptor(true),
      helper.getSearchBarInterceptor(true, searchBarApi),
    ]);

    cy.login();
    cy.skipIntroducePage();
  };

  it("should initialize query and filters from url on search page", () => {
    setupTest();

    helper.goToSearchPage({
      "filter__entityType␞typeNames___false___EQUAL___0": "DATASET",
      filter_domains___false___EQUAL___1: "urn:li:domain:marketing",
      filter_owners___false___EQUAL___3: "urn:li:corpuser:datahub",
      filter_platform___false___EQUAL___2: "urn:li:dataPlatform:kafka",
      filter_tags___false___EQUAL___4: "urn:li:tag:Cypress",
      page: "1",
      query: SAMPLE_ENTITY_NAME,
      unionType: "0",
    });

    helper.searchBarV2.click();

    helper.searchBarV2.ensureTextInSearchBar(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.filters.ensureValuesSelected("platform", ["Kafka"]);
    helper.searchBarV2.filters.ensureValuesSelected("entity-type", ["Dataset"]);
    helper.searchBarV2.filters.ensureValuesSelected("owner", ["DataHub"]);
    helper.searchBarV2.filters.ensureValuesSelected("tag", ["Cypress"]);
    helper.searchBarV2.filters.ensureValuesSelected("domain", ["Marketing"]);
  });

  it("should initialize filters from search bar filter on search page", () => {
    setupTest();

    helper.goToHomePage();
    helper.searchBarV2.type(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.filters.apply("platform", ["Kafka"]);
    helper.searchBarV2.clickOnViewAllSection();

    helper.ensureItIsSearchPage();

    helper.ensureFilterAppliedOnSearchPage("Platform", "Kafka");
  });
});