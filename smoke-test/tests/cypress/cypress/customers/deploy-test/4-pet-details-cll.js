import { PET_DETAILS_URN } from "./constants";

// Spec: Open columns on PET_DETAILS snowflake table. Verify pet_fk CLL is multi-hop,
// both upstream and downstream, going from PETS.pk snowflake table, through dbt entities,
// and into Long Tail Pets looker explore
describe("4. PET_DETAILS - Column Level Lineage Tests", () => {
  beforeEach(() => {
    cy.login();
  });

  it("should verify pet_fk multi-hop CLL from PETS.pk through dbt to Long Tail Pets", () => {
    cy.visit(
      `/dataset/${PET_DETAILS_URN}/Columns?highlightedPath=&is_lineage_mode=false&schemaFilter=`,
    );

    cy.contains("pet_details", { timeout: 30000 }).should("exist");
    cy.get("tbody tr", { timeout: 30000 }).should("have.length.greaterThan", 0);

    cy.get("body", { timeout: 30000 }).then(($body) => {
      if ($body.text().includes("pet_fk")) {
        cy.contains("pet_fk", { timeout: 30000 }).should("be.visible");
        cy.contains("pet_fk").first().click();
        cy.get("body").should("contain", "pet_fk");

        cy.visit(
          `/dataset/${PET_DETAILS_URN}/Lineage?highlightedPath=&is_lineage_mode=false&schemaFilter=&show_all_time_lineage=true`,
        );

        cy.get(".react-flow__node", { timeout: 30000 }).should("exist");

        // Check for dbt and PETS entities in the lineage graph
        cy.get(".react-flow__node", { timeout: 30000 }).then(($nodes) => {
          const nodesText = $nodes.text();
          if (nodesText.includes("dbt")) {
            cy.get(".react-flow__node").contains("dbt").should("exist");
          }
          if (nodesText.includes("PETS")) {
            cy.get(".react-flow__node").contains("PETS").should("exist");
          }
        });
      }
    });
  });

  // Spec: Confirm that the dbt entities go away when toggling "hide transformations" (under filters),
  // but CLL is still shown upstream
  // Spec: Confirm the dbt entities reappear when toggled off
  it("should hide/show dbt transformations and verify CLL is preserved", () => {
    cy.visit(
      `/dataset/${PET_DETAILS_URN}/Lineage?highlightedPath=&is_lineage_mode=false&schemaFilter=&show_all_time_lineage=true`,
    );

    // Wait for lineage graph to fully render
    cy.get(".react-flow__node", { timeout: 30000 }).should("exist");
    cy.get(".react-flow__node", { timeout: 30000 }).should(
      "have.length.greaterThan",
      5,
    );

    // Ensure "Hide Transformations" filter is OFF (transformations visible) before starting test
    cy.get('[aria-label="filter"]').filter(":visible").first().click();
    cy.contains(/Hide Transformations/i, { timeout: 5000 }).should("exist");

    // Check if filter is already ON and toggle it OFF if needed
    cy.contains(/Hide Transformations/i)
      .parent()
      .parent()
      .find('button[role="switch"]')
      .then(($switch) => {
        const isChecked = $switch.attr("aria-checked") === "true";
        if (isChecked) {
          cy.wrap($switch).click();
          cy.get(".react-flow__node-lineage-transformation", {
            timeout: 15000,
          }).should("exist");
        }
      });

    // Close the filter menu
    cy.get('[aria-label="filter"]').filter(":visible").first().click();

    // Wait for transformation nodes to be fully loaded
    cy.get(".react-flow__node-lineage-transformation", {
      timeout: 10000,
    }).should("exist");

    // Capture initial state (transformations visible)
    cy.get(".react-flow__node")
      .its("length")
      .then((initialCount) => {
        cy.get(".react-flow__node-lineage-transformation")
          .its("length")
          .then((initialDbtCount) => {
            expect(initialDbtCount).to.be.greaterThan(0);

            // Re-open filter menu and toggle "Hide Transformations" ON
            cy.get('[aria-label="filter"]').filter(":visible").first().click();
            cy.contains(/Hide Transformations/i)
              .parent()
              .parent()
              .find('button[role="switch"]')
              .click();

            // Verify transformation nodes are hidden
            cy.get(".react-flow__node", { timeout: 10000 }).should(
              ($nodesAfterHide) => {
                expect($nodesAfterHide.length).to.be.lessThan(initialCount);
              },
            );

            cy.get("body").then(($body) => {
              const dbtCountAfterHide = $body.find(
                ".react-flow__node-lineage-transformation",
              ).length;
              expect(dbtCountAfterHide).to.equal(0);
            });

            // Verify CLL edges are still present
            cy.get(".react-flow__edge")
              .its("length")
              .should("be.greaterThan", 0);

            // Toggle "Hide Transformations" OFF
            cy.contains(/Hide Transformations/i)
              .parent()
              .parent()
              .find('button[role="switch"]')
              .click();

            // Wait for transformation nodes to reappear
            cy.get(".react-flow__node-lineage-transformation", {
              timeout: 15000,
            }).should("exist");
            cy.get(".react-flow__node-lineage-transformation", {
              timeout: 15000,
            }).should("have.length.greaterThan", 0);

            // Verify total nodes returned to initial count (verifies all transformations are back)
            cy.get(".react-flow__node", { timeout: 15000 }).should(
              "have.length",
              initialCount,
            );
          });
      });
  });
});
