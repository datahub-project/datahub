import { aliasQuery, hasOperationName } from "../utils";

const WORKFLOW_NAME = "Cypress Entrypoint Test Workflow";
const TEST_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:hive,cypress.entrypoint.test.dataset,PROD)";

describe("Workflow Entrypoint Display Bug Fix", () => {
  let workflowUrn = null;
  let requestUrn = null;

  beforeEach(() => {
    cy.on("uncaught:exception", () => false);
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setWorkflowsEnabledFlags = () => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.reply((res) => {
          res.body.data.appConfig.featureFlags.actionWorkflowsEnabled = true;
          res.body.data.appConfig.featureFlags.showTaskCenterRedesign = true;
          res.body.data.appConfig.featureFlags.themeV2Enabled = true;
          res.body.data.appConfig.featureFlags.themeV2Default = true;
        });
      }
    });
  };

  // GraphQL helper that uses session cookies (must be called after login)
  const graphqlRequest = (query, variables = {}) =>
    cy.request({
      method: "POST",
      url: "/api/v2/graphql",
      body: { query, variables },
      failOnStatusCode: false,
    });

  const createWorkflowWithEntityProfileOnly = () => {
    const mutation = `
      mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
          urn
          name
        }
      }
    `;

    const variables = {
      input: {
        name: WORKFLOW_NAME,
        category: "ACCESS",
        description:
          "Test workflow with ONLY ENTITY_PROFILE entrypoint (no HOME)",
        trigger: {
          type: "FORM_SUBMITTED",
          form: {
            entityTypes: ["DATASET"],
            fields: [
              {
                id: "reason",
                name: "Access Reason",
                description: "Why do you need access?",
                valueType: "STRING",
                cardinality: "SINGLE",
                required: true,
              },
            ],
            entrypoints: [
              {
                type: "ENTITY_PROFILE",
                label: "Request Access (Entity Profile Only)",
              },
              // NOTE: Intentionally NO HOME entrypoint - this is what triggers the bug
            ],
          },
        },
        steps: [
          {
            id: "approval-step",
            type: "APPROVAL",
            description: "Approval step",
            actors: {
              // Use 'admin' user which is the CYPRESS_ADMIN_USERNAME
              userUrns: ["urn:li:corpuser:admin"],
              groupUrns: [],
              roleUrns: [],
            },
          },
        ],
      },
    };

    return graphqlRequest(mutation, variables).then((response) => {
      if (response.body.errors) {
        throw new Error(
          `Failed to create workflow: ${JSON.stringify(response.body.errors)}`,
        );
      }
      const { urn } = response.body.data.upsertActionWorkflow;
      return cy.wrap(urn);
    });
  };

  const createActionRequest = (wfUrn) => {
    const mutation = `
      mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
      }
    `;

    const variables = {
      input: {
        workflowUrn: wfUrn,
        entityUrn: TEST_DATASET_URN,
        fields: [
          { id: "reason", values: [{ stringValue: "Cypress test request" }] },
        ],
      },
    };

    return graphqlRequest(mutation, variables).then((response) => {
      if (response.body.errors) {
        throw new Error(
          `Failed to create request: ${JSON.stringify(response.body.errors)}`,
        );
      }
      const reqUrn = response.body.data.createActionWorkflowFormRequest;
      return cy.wrap(reqUrn);
    });
  };

  const findWorkflowByName = (name) => {
    const query = `
      query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
          workflows {
            urn
            name
          }
        }
      }
    `;

    return graphqlRequest(query, { input: { start: 0, count: 100 } }).then(
      (response) => {
        const workflows =
          response.body.data?.listActionWorkflows?.workflows || [];
        const found = workflows.find((w) => w.name === name);
        return cy.wrap(found?.urn || null);
      },
    );
  };

  afterEach(() => {
    // Clean up in reverse order: request first, then workflow
    if (requestUrn) {
      cy.deleteUrn(requestUrn);
      requestUrn = null;
    }
    if (workflowUrn) {
      cy.deleteUrn(workflowUrn);
      workflowUrn = null;
    }
  });

  it("displays workflow name in Task Center for ENTITY_PROFILE-only workflows", () => {
    setWorkflowsEnabledFlags();

    // Login first to get session cookies
    cy.login();

    // Pre-cleanup: remove any leftover test data from previous failed runs
    findWorkflowByName(WORKFLOW_NAME).then((existingUrn) => {
      if (existingUrn) {
        cy.log(`Cleaning up existing workflow: ${existingUrn}`);
        cy.deleteUrn(existingUrn);
        cy.wait(2000);
      }
    });

    // Step 1: Create workflow with ONLY ENTITY_PROFILE entrypoint
    createWorkflowWithEntityProfileOnly().then((urn) => {
      workflowUrn = urn;
      cy.log(`Created workflow: ${workflowUrn}`);

      // Wait for the workflow to be indexed
      cy.wait(3000);

      // Step 2: Create an action request for this workflow
      createActionRequest(workflowUrn).then((reqUrn) => {
        requestUrn = reqUrn;
        cy.log(`Created action request: ${requestUrn}`);

        // Wait for the request to be indexed
        cy.wait(3000);

        // Step 3: Visit Task Center proposals tab
        cy.visit("/requests/proposals");
        cy.wait(5000);

        // Step 4: Verify the workflow name is displayed (NOT "unknown (deleted workflow)")
        // The bug would show "unknown (deleted workflow)" because the workflow has no HOME entrypoint
        cy.contains(WORKFLOW_NAME, { timeout: 15000 }).should("be.visible");
        cy.contains("unknown (deleted workflow)").should("not.exist");

        cy.log("✓ Bug fix verified: Workflow name is displayed correctly");
      });
    });
  });
});
