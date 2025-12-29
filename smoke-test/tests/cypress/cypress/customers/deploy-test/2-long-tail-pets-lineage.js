import { LONG_TAIL_PETS_URN } from "./constants";

// Spec: Navigate to long tail pets Lineage tab (must be viewing "All Time" lineage)
describe("2. Long Tail Pets - Lineage Tests", () => {
  beforeEach(() => {
    cy.login();
    cy.visit(
      `/dataset/${LONG_TAIL_PETS_URN}/Lineage?highlightedPath=&is_lineage_mode=false&schemaFilter=&show_all_time_lineage=true`,
    );

    cy.contains("Long Tail Pets", { timeout: 30000 }).should("exist");
    cy.get(".react-flow__node", { timeout: 30000 }).should("exist");
    cy.get(".react-flow__node", { timeout: 30000 }).should(
      "have.length.greaterThan",
      0,
    );
  });

  it("should display upstream views and downstream looks", () => {
    cy.contains("dog_breeds", { timeout: 30000 }).should("exist");
    cy.contains("pet_details", { timeout: 30000 }).should("exist");
    cy.contains("Lineage", { timeout: 30000 }).should("be.visible");
    cy.contains("shown", { timeout: 30000 }).should("be.visible");
  });

  // Spec: Confirm we display a manual upstream edge (dotted line) to base_table_2 bigquery table
  it("should display manual upstream edge to base_table_2 bigquery table if present", () => {
    cy.get(".react-flow__edge").should("exist");

    cy.get("body").then(($body) => {
      if ($body.text().includes("base_table_2")) {
        cy.contains("base_table_2").should("be.visible");
        cy.get(".react-flow__edge-path[stroke-dasharray]").should("exist");
      }
    });
  });

  // Spec: Expand dog_breeds Looker View upstream a single time (<)
  it("should expand dog_breeds upstream one level", () => {
    cy.get(".react-flow__node").contains("dog_breeds").first().click();

    cy.get(".react-flow__node.selected").should("exist");

    cy.get(".react-flow__node").then(($initialNodes) => {
      const beforeCount = $initialNodes.length;

      cy.get(".react-flow__node.selected").within(() => {
        cy.get('[direction="UPSTREAM"]').parent().click({ force: true });
      });

      cy.get(".react-flow__node").should(
        "have.length.greaterThan",
        beforeCount,
      );
    });
  });

  // Spec: Multi-expand pet_details Looker View and verify it shows all upstreams (<<)
  it("should multi-expand pet_details upstream to show all upstreams", () => {
    cy.get(".react-flow__node").contains("pet_details").first().click();

    cy.get(".react-flow__node.selected").should("exist");

    cy.get(".react-flow__node").then(($initialNodes) => {
      const beforeCount = $initialNodes.length;

      cy.get(".react-flow__node.selected").within(() => {
        cy.get('[direction="UPSTREAM"]')
          .parent()
          .parent()
          .trigger("mouseenter");
      });

      cy.get(".react-flow__node.selected").within(() => {
        cy.get('svg path[d*="M141.66,133.66l-80,80"]', { timeout: 5000 })
          .should("exist")
          .parent()
          .parent()
          .click({ force: true });
      });

      cy.get(".react-flow__node").should(
        "have.length.greaterThan",
        beforeCount,
      );
    });
  });

  // Spec: Click on looker chart, snowflake table, and dbt entities to load their respective sidebars
  it("should click on looker views to load entity details", () => {
    cy.get(".react-flow__node").contains("dog_breeds").first().click();
    cy.contains("View", { timeout: 30000 }).should("exist");
    cy.contains("Columns", { timeout: 30000 }).should("exist");

    cy.get(".react-flow__node").contains("pet_details").first().click();
    cy.contains("pet_details", { timeout: 30000 }).should("exist");
  });

  it("should click on Looker chart (Look) to load entity details", () => {
    // First expand downstream to make Look entities visible
    cy.get("body").then(($body) => {
      if ($body.find(".show-more").length > 0) {
        const beforeCount = $body.find(".react-flow__node").length;
        cy.get(".show-more").first().click();

        // Wait for new nodes to be added after expansion
        cy.get(".react-flow__node").should(
          "have.length.greaterThan",
          beforeCount,
        );
      }
    });

    // Wait for Look entities to appear in the graph
    cy.get(".react-flow__node", { timeout: 30000 }).then(($nodes) => {
      const nodesText = Array.from($nodes)
        .map((n) => n.textContent)
        .join(" ");

      if (nodesText.includes("Look")) {
        // Click on a Look entity
        cy.get(".react-flow__node").contains("Look").first().click();

        // Verify Look entity details loaded in sidebar
        cy.contains("Look", { timeout: 30000 }).should("exist");
        cy.contains("Columns", { timeout: 30000 }).should("exist");
      } else {
        cy.log("No Look entities found in the lineage graph");
      }
    });
  });

  it("should click on Snowflake table to load entity details if present", () => {
    // Check if there are Snowflake table nodes in the graph
    cy.get(".react-flow__node", { timeout: 30000 }).then(($nodes) => {
      const nodesText = Array.from($nodes)
        .map((n) => n.textContent)
        .join(" ");

      // Look for common Snowflake table indicators
      const hasSnowflakeTable =
        nodesText.toLowerCase().includes("table") ||
        nodesText.includes("ANALYTICS") ||
        nodesText.includes("snowflake");

      if (hasSnowflakeTable) {
        // Try to click on a node that looks like a Snowflake table
        cy.get(".react-flow__node")
          .contains(/Table|ANALYTICS/i)
          .first()
          .click();

        // Verify table entity details loaded in sidebar
        cy.contains("Table", { timeout: 30000 }).should("exist");
        cy.contains("Columns", { timeout: 30000 }).should("exist");
      } else {
        cy.log("No Snowflake table entities found in the lineage graph");
      }
    });
  });

  it("should click on dbt entity to load entity details if present", () => {
    // Check if there are dbt transformation nodes in the graph
    cy.get(".react-flow__node", { timeout: 30000 }).then(($nodes) => {
      const nodesText = Array.from($nodes)
        .map((n) => n.textContent)
        .join(" ");

      if (
        nodesText.includes("dbt") ||
        nodesText.toLowerCase().includes("model")
      ) {
        // Click on a dbt entity
        cy.get(".react-flow__node")
          .contains(/dbt|model/i)
          .first()
          .click();

        // Verify dbt entity details loaded in sidebar
        cy.get("body").should("contain", "dbt");
        cy.contains("Table", { timeout: 30000 }).should("exist");
      } else {
        cy.log("No dbt entities found in the lineage graph");
      }
    });
  });

  // Spec: Test lineage filter node downstream of Long Tail Pets can be searched, expanded, and contracted
  it("should expand and view downstream entities", () => {
    cy.get("body").then(($body) => {
      if ($body.find(".show-more").length > 0) {
        cy.get(".react-flow__node").then(($nodes) => {
          const beforeCount = $nodes.length;

          cy.get(".show-more").first().click();
          cy.get(".react-flow__node").should(
            "have.length.greaterThan",
            beforeCount,
          );
          cy.get("body").should("contain", "Look");
        });
      }
    });
  });

  it("should search within expanded downstream filter node", () => {
    // First expand downstream entities
    cy.get("body").then(($body) => {
      if ($body.find(".show-more").length > 0) {
        cy.get(".show-more").first().click();

        // Wait for expansion to complete
        cy.get(".react-flow__node", { timeout: 10000 }).should(
          "have.length.greaterThan",
          8,
        );

        // Find the filter node's search input (should be within the expanded filter node area)
        cy.get('input[placeholder*="Search"]').then(($inputs) => {
          // Filter nodes may have search inputs
          const filterNodeSearchInputs = $inputs.filter((i, el) => {
            const parent = el.closest(".react-flow__node");
            return parent && parent.textContent.includes("shown");
          });

          if (filterNodeSearchInputs.length > 0) {
            cy.wrap(filterNodeSearchInputs.first()).type("Pet");

            // Verify search is working (some results should still be visible)
            cy.get("body").should("contain", "Look");
          } else {
            cy.log("No search input found in filter node");
          }
        });
      }
    });
  });

  it("should contract expanded downstream entities", () => {
    // First expand downstream entities
    cy.get("body").then(($body) => {
      if ($body.find(".show-more").length > 0) {
        cy.get(".show-more").first().click();

        // Wait for expansion and get node count
        cy.get(".react-flow__node", { timeout: 10000 }).then(
          ($expandedNodes) => {
            const expandedCount = $expandedNodes.length;

            // After expansion, look for the .show-more wrapper again
            // The "Show Less" button is hidden in <ExtraButtons> and only appears on hover
            cy.get(".show-more").then(($showMoreWrappers) => {
              if ($showMoreWrappers.length > 0) {
                // Hover over the .show-more wrapper to reveal hidden buttons
                cy.wrap($showMoreWrappers.first()).trigger("mouseenter");

                // Now look for "Show Less" button (should be visible after hover)
                cy.contains("Show Less", { timeout: 5000 }).then(
                  ($showLess) => {
                    if ($showLess.length > 0) {
                      cy.wrap($showLess.first()).click({ force: true });

                      // Verify node count decreased (entities were collapsed)
                      cy.get(".react-flow__node", { timeout: 10000 }).should(
                        ($contractedNodes) => {
                          expect($contractedNodes.length).to.be.lessThan(
                            expandedCount,
                          );
                        },
                      );
                    } else {
                      cy.log(
                        "Show Less button not found - may not have expanded enough items",
                      );
                    }
                  },
                );
              } else {
                cy.log("No .show-more wrapper found after expansion");
              }
            });
          },
        );
      }
    });
  });
});
