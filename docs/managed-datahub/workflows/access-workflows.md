import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Data Access Workflows

<!-- All Feature Guides should begin with `About DataHub ` to improve SEO -->

<!--
This feature is currently in Private Beta - requires outreach to DataHub team to enable
-->

<FeatureAvailability saasOnly/>

> **Note**: Access Workflows is currently in **Private Beta**. To enable this feature, please reach out to the DataHub team.

Data Access Workflows enable organizations to create centralized approval processes for all data access requests, ensuring compliance while streamlining the request and review experience.

## Key Capabilities

**Create Approval Workflows**: Design access request workflows with custom fields, entry points, and multi-step approval processes. Define which types of assets users can request access to and configure routing rules for reviewers.

**Keep Everyone in the Loop**: Stay informed with notifications via email or Slack when you have requests to review, or when a request you created is approved or denied. Monitor all your open requests directly in your Task Center.

**Realtime, Event-Oriented Integration**: Provision access in real-time by tuning into events emitted when access requests are created or reviewed via the DataHub Actions Framework.

## Typical Use Cases

- **Centralized Access Requests**: Enable access requests across all data systems for tables and dashboards, or higher-level groups like domains, data products, and databases - all through a single interface
- **Audit Log**: View history of all access requests in one place to audit access grants over time and maintain compliance records
- **Dynamic Approval Routing**: Automatically route requests to appropriate reviewers based on asset ownership or organizational structure
- **Data Access Governance**: Implement controlled access to sensitive datasets with proper approval chains and organizational policies

## Typical Users

- **Data Platform Administrators**: Configure and manage workflow definitions
- **Data Stewards**: Review and approve access requests for their domains
- **Data Consumers**: Submit access requests for datasets they need
- **Integration Engineers**: Set up automated provisioning based on workflow events

## Access Workflows Setup, Prerequisites, and Permissions

<!-- This section should provide plain-language instructions on how to configure the feature:

* What special configuration is required, if any?
* How can you confirm you configured it correctly? What is the expected behavior?
* What access levels/permissions are required within DataHub? -->

### Prerequisites

- Access Workflows feature must be enabled by the DataHub team (Private Beta)
- DataHub instance with GraphQL API access
- Administrative privileges to create workflow definitions
- Configured notification channels (Email/Slack) for workflow notifications - enabled by default

### Required Permissions

- **Create Workflows**: Platform Admin or user with `Manage Workflows` privilege
- **Submit Requests**: Any authenticated user (subject to workflow visibility rules)
- **Review Requests**: Users assigned as reviewers in workflow definitions
- **Configure Notifications**: Individual users can configure their own notification preferences

<!-- Plain-language instructions of how to use the feature

Provide a step-by-step guide to use feature, including relevant screenshots and/or GIFs

* Where/how do you access it?
* What best practices exist?
* What are common code snippets?
 -->

## Creating an Access Workflow

Currently, workflow creation is must be done via the GraphQL API using the `upsertActionWorkflow` mutation.

Here's a Python example that uses the DataHub Python client to create a basic dataset approval workflow:

<details>
<summary>Create Access Workflow In Python</summary>

```python
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Initialize DataHub client
config = DatahubClientConfig(
    server="http://your-datahub-instance",
    token="YOUR_ACCESS_TOKEN"
)
graph = DataHubGraph(config)

# GraphQL mutation for creating an approval workflow
CREATE_WORKFLOW_MUTATION = """
mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
  upsertActionWorkflow(input: $input) {
    urn
  }
}
"""

workflow_definition = {
  "name": "Dataset Access Request",
  "description": "Request access to sensitive datasets",
  "category": "ACCESS",
  "trigger": {
      "type": "FORM_SUBMITTED",
      "form": {
          "entityTypes": ["DATASET"],  # Limit to dataset entities, but can apply to many types.
          "entrypoints": [
              {
                  "type": "HOME", # Display on Home Page
                  "label": "Request Dataset Access" # Home Page CTA
              },
              {
                  "type": "ENTITY_PROFILE", # Display on Entity Profile Page
                  "label": "Request Access" # Entity Profile Page CTA
              }
          ],
          "fields": [
              {
                  "id": "business_justification",
                  "name": "Business Justification",
                  "description": "Please explain why you need access to this dataset",
                  "valueType": "RICH_TEXT",
                  "cardinality": "SINGLE",
                  "required": True
              },
              {
                  "id": "access_duration",
                  "name": "Access Duration",
                  "description": "How long do you need access?",
                  "valueType": "STRING",
                  "allowedValues": [
                      {"stringValue": "30_DAYS"},
                      {"stringValue": "90_DAYS"},
                      {"stringValue": "PERMANENT"}
                  ],
                  "cardinality": "SINGLE",
                  "required": False
              },
              # Create a conditionally visible field. Only visible based on previous field answer.
              {
                  "id": "permanent_access_justification",
                  "name": "Permanent Access Justification",
                  "description": "Since you've requested permanent access, please provide additional justification for why this is necessary",
                  "valueType": "RICH_TEXT",
                  "cardinality": "SINGLE",
                  "required": True,
                  "condition": {
                      "type": "SINGLE_FIELD_VALUE",
                      "singleFieldValueCondition": {
                          "field": "access_duration",
                          "values": ["PERMANENT"],
                          "condition": "EQUAL",
                          "negated": False
                      }
                  }
              }
          ]
      }
  },
  "steps": [
      {
          "id": "data_steward_review",
          "type": "APPROVAL",
          "description": "Data steward review and approval",
          "actors": {
              "userUrns": ["urn:li:corpuser:data.steward"],
              "groupUrns": [],
              "roleUrns": [],
              "dynamicAssignment": {
                  "type": "ENTITY_OWNERS"
              }
          }
      }
  ]
}

# Workflow definition
workflow_input = {
    "input": workflow_definition
}

# Execute the mutation
try:
    result = graph.execute_graphql(
        query=CREATE_WORKFLOW_MUTATION,
        variables=workflow_input
    )
    print(f"Workflow created successfully: {result['upsertActionWorkflow']['urn']}")
    print(f"Workflow name: {result['upsertActionWorkflow']['name']}")
except Exception as e:
    print(f"Error creating workflow: {e}")
```

</details>

### Workflow Concepts

**Entry Points**: Define where users can initiate the workflow

- `HOME`: Workflow appears on the home page to all users
- `ENTITY_PROFILE`: Workflow appears on entity detail pages

**Field Types**: Supported form field types

- `STRING`: Single-line text input
- `RICH_TEXT`: Multi-line rich text input with formatting
- `URN`: Entity reference (user, group, dataset, etc.) with configurable entity types
- `DATE`: Date/time value represented as epoch timestamp in milliseconds
- `NUMBER`: Numeric input (integer or float)

**Field Cardinality**: Controls whether fields accept single or multiple values

- `SINGLE`: Field accepts only one value
- `MULTIPLE`: Field accepts multiple values

**Assignee Resolution**: Configure who reviews workflow requests

- **Static Assignment**: Assign specific users, groups, and roles to review
  - `userUrns`: Specific users assigned to review
  - `groupUrns`: Specific groups assigned to review
  - `roleUrns`: Specific DataHub roles assigned to review
- **Dynamic Assignment**: Automatically resolve reviewers based on entity context
  - `ENTITY_OWNERS`: Assign to the owners of the requested entity
  - `ENTITY_DOMAIN_OWNERS`: Assign to the owners of the entity's domain
  - `ENTITY_DATA_PRODUCT_OWNERS`: Assign to the owners of the entity's data product
  - Optional `ownershipTypes`: Filter by specific ownership types (e.g., Technical Owner, Business Owner)

**Categories**:

- `ACCESS`: Access-related workflows
- `CUSTOM`: Custom workflows with user-defined `customCategory` string. For example, to model data creation requests.

## Submitting Access Workflow Request

Once a workflow is created, users will be able to submit a Workflow Request form to trigger the review process. Depending on the entry point specified for the workflow, users will be able to start the workflow from either the

1. Home Page
2. Entity Profile

To create an approval workflow request, users must simply provide responses for all required fields.

Once completed, it can be submitted by clicking "Submit".

<p align="center">
  <img width="70%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/workflows/create-action-workflow-entity-profile.png"/>
</p>

Once a request is submitted, your open requests will be visible from within **Tasks** > **Requests** > **My Requests**.

### Reviewing Approval Workflow Requests

Users assigned as reviewers can manage requests through the **Task Center**:

### Accessing the Task Center

1. From the Navigation sidebar, click "Tasks"
2. Click on the "Requests" tab

### Reviewing a Request

1. **View Request Details**: Click "View Details" on any pending request to see:
   - Requestor information
   - Selected entity
   - Submitted form responses

<p align="center">
  <img width="70%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/workflows/review-action-workflow-request-details.png"/>
</p>

2. **Make a Decision**: For each request, you can:

   - **Accept**: Grant the access request
   - **Reject**: Deny the request with optional comments

3. **Add Comments**: Provide context for your decision to help requestors understand the outcome

<p align="center">
  <img width="70%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/workflows/review-action-workflow-request-confirm.png"/>
</p>

## Getting Notified About Access Workflows

To stay informed about workflow activities, you can configure notifications:

1. Navigate to **Settings** > **My Notifications**
2. Find the **Workflow Notifications** section
3. Enable notifications in the following cases:
   - **Pending Reviews**: Get notified when you have requests to review
   - **Request Updates**: Get notified when your submitted requests are approved or denied

## Access Provisioning: Reacting to Approval Workflow Events

Integrate with the [DataHub Actions Framework](https://docs.datahub.com/docs/actions) to automate access provisioning based on workflow events.

### Event Types

DataHub emits events for key workflow lifecycle moments:

1. **Approval Workflow Request Create Event**: When a workflow request form is submitted
2. **Approval Workflow Request Step Complete**: When an intermediate review step is completed
3. **Approval Workflow Request Complete**: When the workflow request is fully approved or rejected

The format of each JSON event can be found by visiting the

### Setting Up Event Integration

Create a custom [DataHub Actions](https://docs.datahub.com/docs/actions) listener for workflow events to trigger custom access provisioning.

To get started by printing these events out:

```
name: "access-workflow-provisioner-action"
datahub:
  server: "your-datahub-server"
  token: "your-access-token"
source:
  type: "datahub-cloud"
# Add filter to filter down to just access request lifecycle events.
filter:
    event_type: "EntityChangeEvent_v1"
    event:
        entityType: "actionRequest"
        category: "LIFECYCLE"
        operation: "COMPLETED" # OR CREATE OR MODIFY
        parameters:
            actionRequestType: "WORKFLOW_FORM_REQUEST"
action:
  type: "hello_world"
  config: {}
```

In reality, this action would likely respond by making the changes required to provision access for the requesting user.

For example, by:

- Adding the user to a specific LDAP group
- Assigning a particular Snowflake role for the user
- Generating an AWS IAM policy granting access to an S3 bucket

For full documentation on building a custom action, check out [Developing an Action](https://docs.datahub.com/docs/actions/guides/developing-an-action).

### Event Schema Reference

For event schemas & examples, see [Entity Change Events](https://docs.datahub.com/docs/actions/events/entity-change-event).

## Additional Resources

<!-- Comment out any irrelevant or empty sections -->

### GraphQL

- **upsertActionWorkflow** - Create or update workflow definitions
- **createActionWorkflowFormRequest** - Submit workflow requests
- **reviewActionWorkflowFormRequest** - Review workflow requests
- **deleteActionWorkflow** - Delete workflow definitions

To see the full schema types and documentation, visit GraphiQL at https://<your-datahub-instance>.acryl.io/api/graphiql and view definitions under the `Mutation` type. 

## FAQ and Troubleshooting

<!-- Use the following format:

**Question in bold text**

Response in plain text

-->

**How do I enable Approval Workflows for my organization?**

Approval Workflows is currently in Private Beta. Contact the DataHub team to request access to this feature for your instance.

**Can I create workflows that don't require entity context?**

Yes, by omitting the `entityTypes` field in your workflow definition, you can create general workflows that don't require a specific entity context.

**How do I configure dynamic assignee resolution?**

Configure dynamic assignment in your workflow step's `actors` section using the `dynamicAssignment` field. Available types include:

- `ENTITY_OWNERS`: Route to owners of the requested entity
- `ENTITY_DOMAIN_OWNERS`: Route to owners of the entity's domain
- `ENTITY_DATA_PRODUCT_OWNERS`: Route to owners of the entity's data product

You can optionally filter by specific ownership types using the `ownershipTypeUrns` field.

**What field types are supported in workflow forms?**

Access Workflows support the following field types:

- `STRING`: Single-line text input
- `RICH_TEXT`: Multi-line rich text with formatting
- `URN`: Entity references (configurable by entity type)
- `DATE`: Date/time values (epoch milliseconds)
- `NUMBER`: Numeric inputs (integer or float)

Each field can be configured for single or multiple values using the `cardinality` property.

**Can I create custom workflow categories?**

Yes, use the `CUSTOM` category and provide a `customCategory` string to group related workflows outside of the standard `ACCESS` category.

**What happens if a reviewer is unavailable?**

You can configure multiple reviewers in a single step to increase the chance that a user can review at each step.

**Can I modify a workflow after it's been created?**

Yes, use the `upsertActionWorkflow` mutation with the existing workflow URN to update the definition. Note that changes only affect new requests, not existing ones.

**How do I handle requests that require multiple approval steps?**

Define multiple steps in your workflow configuration. Each step can have different assignees and approval requirements.

**Can users cancel their own requests?**

Currently, request cancellation is managed through the review process. Users can contact their reviewers to withdraw requests if needed.
