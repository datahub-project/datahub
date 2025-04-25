import { applyGraphqlInterceptors, getThemeV2Interceptor } from "../utils";
import { SearchV2Helper } from "./helpers/searchV2Helper";

const SAMPLE_ENTITY_NAME = "SampleCypressKafkaDataset";
const SAMPLE_ENTITY_URN =
  "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)";

describe("searchBarV2", () => {
  const helper = new SearchV2Helper();

  const setupTest = (searchBarApi = "AUTOCOMPLETE_FOR_MULTIPLE") => {
    cy.on(
      "uncaught:exception",
      (err) =>
        !err.message.includes(
          "ResizeObserver loop completed with undelivered notifications",
        ),
    );

    applyGraphqlInterceptors([
      getThemeV2Interceptor(true),
      helper.getSearchBarInterceptor(true, searchBarApi),
    ]);

    cy.loginWithCredentials();
  };

  it("can handle all entities search", () => {
    setupTest();
    helper.goToHomePage();

    helper.searchBarV2.type("*{enter}");
    helper.waitForSearchPageLoading();

    helper.ensureHasResults();
  });

  it("can handle all entities search with an impossible query and find 0 results", () => {
    setupTest();
    helper.goToHomePage();

    helper.searchBarV2.type("zzzzzzzzzzzzzqqqqqqqqqqqqqzzzzzzqzqzqzqzq{enter}");
    helper.waitForSearchPageLoading();

    helper.ensureHasNoResults();
  });

  it("should show entity in response", () => {
    setupTest();
    helper.goToHomePage();

    helper.searchBarV2.type(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.ensureTextInViewAllSection(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.ensureEntityInResponse(SAMPLE_ENTITY_URN);
  });

  it("should handle clicking on the entity from response", () => {
    setupTest();
    helper.goToHomePage();

    helper.searchBarV2.type(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.clickOnEntityResponseItem(SAMPLE_ENTITY_URN);
    helper.ensureIsOnEntityProfilePage(SAMPLE_ENTITY_URN);
  });

  it("should handle clicking on view all", () => {
    setupTest();
    helper.goToHomePage();

    helper.searchBarV2.type(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.clickOnViewAllSection();

    helper.ensureItIsSeachPage();
    helper.ensureUrlIncludesText(SAMPLE_ENTITY_NAME);
  });

  it("should show no results state", () => {
    setupTest();
    helper.goToHomePage();

    helper.searchBarV2.type(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.filters.apply("platform", ["Looker"]);
    helper.searchBarV2.filters.ensureValuesSelected("platform", ["Looker"]);
    helper.searchBarV2.ensureInNoResultsFoundState();
    helper.searchBarV2.ensureNoResultsFoundStateHasClearButton();
    helper.searchBarV2.clearFiltersInNoResultsFoundState();
    // filters cleared only
    helper.searchBarV2.filters.ensureValuesNotSelected("platform", ["Looker"]);
    helper.searchBarV2.ensureTextInSearchBar(SAMPLE_ENTITY_NAME);

    helper.searchBarV2.clear();
    helper.searchBarV2.type(SAMPLE_ENTITY_NAME + "zzzzzzzzzzzzzzzz");
    helper.searchBarV2.ensureInNoResultsFoundState();
    // without filters there are no clear button
    helper.searchBarV2.ensureNoResultsFoundStateHasNoClearButton();
  });

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
    helper.searchBarV2.focus();

    // The first Esc just closes the search bars dropdown
    helper.searchBarV2.pressEscape();
    helper.searchBarV2.ensureTextInSearchBar(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.ensureClosed();

    // The second Esc cleans query and applied filters in the search bar
    helper.searchBarV2.pressEscape();
    helper.searchBarV2.ensureTextInSearchBar("");
    helper.searchBarV2.ensureClosed();

    // check if platform filter is empty
    helper.searchBarV2.type(SAMPLE_ENTITY_NAME);
    helper.searchBarV2.filters.ensureValuesNotSelected("platform", ["Kafka"]);
  });

  it.only("should apply filters correctly", () => {
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
