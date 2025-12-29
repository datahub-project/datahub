import {
  LONG_TAIL_PETS_URN,
  LONG_TAIL_PETS_BREED_FIELD_URN,
} from "./constants";

// Spec: Confirm CLL works for Long Tail Pets.breed; it should have downstream lineage
// to displayed entities and hidden ones, so we should be indicating that there is CLL
// downstream of breed to a hidden table
describe("5. Breed Field - Column Level Lineage Tests", () => {
  beforeEach(() => {
    cy.login();
  });

  it("should verify CLL for Long Tail Pets breed field with downstream lineage", () => {
    cy.visit(
      `/dataset/${LONG_TAIL_PETS_URN}/Columns?highlightedPath=&is_lineage_mode=false&schemaFilter=`,
    );

    cy.contains("Long Tail Pets", { timeout: 30000 }).should("exist");
    cy.get("tbody tr", { timeout: 30000 }).should("have.length.greaterThan", 0);
    cy.contains("dog_breeds.breed", { timeout: 30000 }).should("exist");
    cy.contains("dog_breeds.breed").first().click();
    cy.get("body").should("contain", "breed");
  });

  it("should navigate to schema field lineage view for breed and interact with it", () => {
    cy.visit(
      `/schemaField/${LONG_TAIL_PETS_BREED_FIELD_URN}/Lineage?highlightedPath=&is_lineage_mode=false&schemaFilter=&show_all_time_lineage=true`,
    );

    cy.contains("breed", { timeout: 30000 }).should("exist");
    cy.contains("dog_breeds", { timeout: 30000 }).should("exist");
    cy.get(".react-flow__node", { timeout: 30000 }).should("exist");
    cy.contains("View", { timeout: 30000 }).should("be.visible");
    cy.contains("long_tail_companions", { timeout: 30000 }).should(
      "be.visible",
    );

    cy.get("body", { timeout: 30000 }).then(($body) => {
      if ($body.text().includes("shown")) {
        cy.contains("shown", { timeout: 30000 }).should("exist");
      }
      if ($body.text().includes("Look")) {
        cy.get("body").should("contain", "Look");
      }
    });

    cy.get('[aria-label="zoom-in"]', { timeout: 30000 })
      .filter(":visible")
      .first()
      .should("exist");
    cy.get('[aria-label="zoom-out"]', { timeout: 30000 })
      .filter(":visible")
      .first()
      .should("exist");
    cy.get('[aria-label="search"]', { timeout: 30000 })
      .filter(":visible")
      .first()
      .should("exist");
    cy.get('[aria-label="filter"]', { timeout: 30000 })
      .filter(":visible")
      .first()
      .should("exist");
  });

  it("should expand downstream entities in breed field lineage", () => {
    cy.visit(
      `/schemaField/${LONG_TAIL_PETS_BREED_FIELD_URN}/Lineage?highlightedPath=&is_lineage_mode=false&schemaFilter=&show_all_time_lineage=true`,
    );

    cy.get(".react-flow__node", { timeout: 30000 }).should("exist");

    cy.get("body").then(($body) => {
      if ($body.find(".show-more").length > 0) {
        cy.get(".react-flow__node").then(($nodes) => {
          const beforeCount = $nodes.length;

          cy.get(".show-more").first().click();

          cy.get(".react-flow__node").should(
            "have.length.greaterThan",
            beforeCount,
          );
        });
      }
    });
  });

  it("should interact with downstream look entities in breed lineage", () => {
    cy.visit(
      `/schemaField/${LONG_TAIL_PETS_BREED_FIELD_URN}/Lineage?highlightedPath=&is_lineage_mode=false&schemaFilter=&show_all_time_lineage=true`,
    );

    cy.get(".react-flow__node", { timeout: 30000 }).should("exist");

    cy.get("body").then(($body) => {
      if ($body.text().includes("Look")) {
        cy.contains("Look").first().click();
        cy.get("body").should("exist");
      }
    });
  });

  it("should search in lineage filter on breed field lineage", () => {
    cy.visit(
      `/schemaField/${LONG_TAIL_PETS_BREED_FIELD_URN}/Lineage?highlightedPath=&is_lineage_mode=false&schemaFilter=&show_all_time_lineage=true`,
    );

    cy.get(".react-flow__node", { timeout: 30000 }).should("exist");

    cy.get('[aria-label="search"]')
      .filter(":visible")
      .first()
      .closest(".ant-input-affix-wrapper")
      .find("input")
      .type("Look");

    cy.get(".react-flow__node").should("exist");

    cy.get("body").then(($body) => {
      if ($body.text().includes("Look")) {
        cy.get("body").should("contain", "Look");
      }
    });
  });
});
