import { PET_DETAILS_URN } from "./constants";

describe("1. Dataset Navigation - PET_DETAILS", () => {
  beforeEach(() => {
    cy.login();
  });

  // Spec: Visit dataset page (PET_DETAILS)
  it("should visit PET_DETAILS dataset page and verify basic info", () => {
    cy.visit(`/dataset/${PET_DETAILS_URN}/Stats?is_lineage_mode=false`);

    cy.contains("pet_details", { timeout: 30000 }).should("exist");
    cy.contains("Columns", { timeout: 15000 }).should("be.visible");
    cy.contains("Lineage", { timeout: 15000 }).should("be.visible");
  });
});
