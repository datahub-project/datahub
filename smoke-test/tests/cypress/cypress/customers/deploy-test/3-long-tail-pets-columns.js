import { LONG_TAIL_PETS_URN } from "./constants";

// Spec: Search and paginate columns on Long Tail Pets
describe("3. Long Tail Pets - Columns and Search", () => {
  beforeEach(() => {
    cy.login();
  });

  it("should navigate to Columns tab and view schema", () => {
    cy.visit(
      `/dataset/${LONG_TAIL_PETS_URN}/Columns?highlightedPath=&is_lineage_mode=false&schemaFilter=`,
    );

    cy.contains("Long Tail Pets", { timeout: 30000 }).should("exist");
    cy.get("tbody tr", { timeout: 30000 }).should("have.length.greaterThan", 0);
    cy.contains("dog_breeds.breed", { timeout: 30000 }).should("exist");
    cy.contains("Name", { timeout: 15000 }).should("exist");
    cy.contains("Type", { timeout: 15000 }).should("exist");
    cy.contains("Description", { timeout: 15000 }).should("exist");

    cy.get('input[placeholder="Search"]', { timeout: 15000 }).type("breed");
    cy.contains("breed", { timeout: 15000 }).should("be.visible");
  });
});
