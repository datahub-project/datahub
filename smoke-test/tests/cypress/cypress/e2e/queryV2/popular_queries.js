const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.dog_rates_twitter,PROD)";
const COLUMN_FILTER = "rating";
const USER_FILTER = "harshal";

const MOST_RECENT = "05/07/2025";
const SECOND_MOST_RECENT = MOST_RECENT; // Use second to avoid tooltip blocking
const SECOND_LEAST_RECENT = /(12\/12\/2024|12\/13\/2024)/; // Changes depending on time zone

describe("browse popular queries", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.skipIntroducePage();
    cy.goToDataset(DATASET_URN, "dog_rates_twitter", true, "Queries");
    cy.waitTextVisible("Popular Queries");
  });

  it("should filter by columns", () => {
    // 20 queries: 4 pages, +2 for prev / next page
    cy.get(".ant-pagination").find("li").should("have.length", 6);
    // QueriesTabSection.Popular === 1
    cy.get('[data-testid="queries-list-section-1"]')
      .find(".ant-table-row")
      .should("have.length", 5); // page size 5

    // Filter
    cy.get('[data-testid="filter-dropdown-Columns"]').click();
    cy.get('[data-testid="filter-dropdown"] [data-testid="search-input"]').type(
      COLUMN_FILTER,
      { delay: 0 },
    );
    cy.get('[data-testid="filter-dropdown"]')
      .find("li")
      .contains(COLUMN_FILTER)
      .click();
    cy.get(
      '[data-testid="filter-dropdown"] [data-testid="update-filters"]',
    ).click();

    // Verify
    cy.get('[data-testid="active-filter-entities"]').contains("Columns");
    cy.get('[data-testid="active-filter-entities"]').contains("equals");
    cy.get('[data-testid="active-filter-entities"]').contains(COLUMN_FILTER);
    // 13 queries: 3 pages, +2 for prev / next page
    cy.get(".ant-pagination").find("li").should("have.length", 5);
    cy.get('[data-testid="queries-list-section-1"]')
      .find(".ant-table-row")
      .should("have.length", 5); // page size 5
    cy.get(".ant-pagination").find("li").contains("3").click();
    cy.get('[data-testid="queries-list-section-1"]')
      .find(".ant-table-row")
      .should("have.length", 3);
  });

  it("should filter by users", () => {
    // 20 queries: 4 pages, +2 for prev / next page
    cy.get(".ant-pagination").find("li").should("have.length", 6);
    // QueriesTabSection.Popular === 1
    cy.get('[data-testid="queries-list-section-1"]')
      .find(".ant-table-row")
      .should("have.length", 5); // page size 5

    // Filter
    cy.get('[data-testid="filter-dropdown-Users"]').click();
    cy.get('[data-testid="filter-dropdown"] [data-testid="search-input"]').type(
      USER_FILTER,
      { delay: 0 },
    );
    cy.get('[data-testid="filter-dropdown"]')
      .find("li")
      .contains(USER_FILTER)
      .click();
    cy.get(
      '[data-testid="filter-dropdown"] [data-testid="update-filters"]',
    ).click();

    // Verify
    cy.get('[data-testid="active-filter-topUsersLast30DaysFeature"]').contains(
      "Users",
    );
    cy.get('[data-testid="active-filter-topUsersLast30DaysFeature"]').contains(
      "equals",
    );
    cy.get('[data-testid="active-filter-topUsersLast30DaysFeature"]').contains(
      USER_FILTER,
    );
    // 2 queries: 1 pages, +2 for prev / next page
    cy.get(".ant-pagination").should("not.exist");
    cy.get('[data-testid="queries-list-section-1"]')
      .find(".ant-table-row")
      .should("have.length", 2);
  });

  it("should sort by last run", () => {
    // Most recent by default
    cy.get('[data-testid="queries-list-section-1"]')
      .find(".ant-table-row")
      .first()
      .find(".lastRun span")
      .trigger("mouseover");
    cy.get(".ant-tooltip-inner").contains(MOST_RECENT).should("be.visible");

    // Sort and verify
    cy.get("th.lastRun").click();
    cy.get(".anticon-loading").should("not.exist");
    cy.get('[data-testid="queries-list-section-1"]')
      .find(".ant-table-row")
      .eq(1)
      .find("td.lastRun div")
      .trigger("mouseover");
    cy.get(".ant-tooltip-inner")
      .contains(SECOND_LEAST_RECENT)
      .should("be.visible");

    // Sort again and verify
    cy.get("th.lastRun").click();
    cy.get(".anticon-loading").should("not.exist");
    cy.get('[data-testid="queries-list-section-1"]')
      .find(".ant-table-row")
      .eq(1)
      .find("td.lastRun div")
      .trigger("mouseover");
    cy.get(".ant-tooltip-inner")
      .contains(SECOND_MOST_RECENT)
      .should("be.visible");
  });

  it("should sort by popularity", () => {
    // Most recent by default
    cy.get('[data-testid="queries-list-section-1"]')
      .find(".ant-table-row")
      .first()
      .find(".popularity [data-testid='query-popularity']")
      .should("be.visible");

    // Sort and verify
    cy.get("th.popularity").click();
    cy.get(".anticon-loading").should("not.exist");
    cy.get('[data-testid="queries-list-section-1"]')
      .find(".ant-table-row")
      .first()
      .find(".popularity [data-testid='query-popularity']")
      .should("not.exist");

    // Sort again and verify
    cy.get("th.popularity").click();
    cy.get(".anticon-loading").should("not.exist");
    cy.get('[data-testid="queries-list-section-1"]')
      .find(".ant-table-row")
      .first()
      .find(".popularity [data-testid='query-popularity']")
      .should("be.visible");
  });
});
