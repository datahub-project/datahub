import { Page } from '@playwright/test';
import { expect, test } from '../../fixtures/base-test';
import { ManageViewsPage } from '../../pages/views/manage-views.page';
import { withRandomSuffix } from '../../utils/random';

test.use({ featureName: 'views' });

const DOMAIN_NAME = 'PlaywrightViewsWithinDomain';
const GLOSSARY_TERM = 'glossaryTerms';
const OPERATOR_EXISTS = 'exists';

interface OperandType {
  type?: string;
  property?: string;
  operator?: string;
  operands?: OperandType[];
}

interface LogicalPredicateType {
  type?: string;
  operator?: string;
  operands?: OperandType[];
}

async function interceptGraphQLRequest(page: Page, operationName: string) {
  const requestPromise = page.waitForRequest((request) => {
    if (!request.url().includes('/api/v2/graphql')) return false;
    const postData = request.postDataJSON() as { operationName?: string } | null;
    return postData?.operationName === operationName;
  });

  const request = await requestPromise;
  const body = request.postDataJSON() as {
    variables?: {
      input?: {
        definition?: {
          filter?: {
            json?: string;
            orFilters?: Array<Record<string, unknown>>;
          };
        };
      };
    };
  };

  const json = body.variables?.input?.definition?.filter?.json || '';
  const orFilters = body.variables?.input?.definition?.filter?.orFilters || [];
  return { json, orFilters };
}

function parseFilterJson(jsonStr: string): LogicalPredicateType {
  return JSON.parse(jsonStr) as LogicalPredicateType;
}

test.describe('View Nested Conditions', () => {
  let manageViewsPage: ManageViewsPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    manageViewsPage = new ManageViewsPage(page, logger, logDir);
    await manageViewsPage.navigate();
  });

  test('builds nested groups with and validates json structure persists through edit', async ({ page }) => {
    const viewName = withRandomSuffix('NestedView');

    await manageViewsPage.createView(viewName);
    await expect(manageViewsPage.conditionSelect).toBeVisible();

    // Create nested structure: AND[Domain, NOT[GlossaryTerms]]
    await manageViewsPage.createNestedGroupWithCondition('domains', DOMAIN_NAME, 'not', GLOSSARY_TERM, OPERATOR_EXISTS);

    // Verify created structure is sent to API in correct format
    const requestPromise = interceptGraphQLRequest(page, 'createView');
    await manageViewsPage.saveView();
    const { json: jsonStr, orFilters } = await requestPromise;

    expect(jsonStr).toBeDefined();
    expect(orFilters).toBeDefined();

    const logicalPredicate = parseFilterJson(jsonStr);

    manageViewsPage.validateNestedStructure(logicalPredicate, {
      expectedRootOperator: 'and',
      nestedPropertyName: GLOSSARY_TERM,
    });

    await manageViewsPage.expectViewVisible(viewName);

    // Open view for edit and verify structure is visible
    const menuButton = await manageViewsPage.getViewOptionMenu(viewName);
    await menuButton.click();
    await manageViewsPage.menuItemEdit.click();

    // Verify nested structure is visible for editing
    await manageViewsPage.verifyStructureForEdit();
    await manageViewsPage.editRootOperator('or');

    // Verify edited structure is persisted correctly
    const updateRequestPromise = interceptGraphQLRequest(page, 'updateView');
    await manageViewsPage.saveView();
    const { json: updatedJsonStr } = await updateRequestPromise;

    expect(updatedJsonStr).toBeDefined();

    const updatedPredicate = parseFilterJson(updatedJsonStr);

    // Validate root operator changed to OR and nested NOT structure persists
    manageViewsPage.validateNestedStructure(updatedPredicate, {
      expectedRootOperator: 'or',
      nestedPropertyName: GLOSSARY_TERM,
    });

    const updatedNestedGroup = (updatedPredicate.operands || []).find((op: OperandType) => op.type === 'logical');
    expect(updatedNestedGroup?.operator?.toLowerCase()).toBe('not');

    // Cleanup
    await manageViewsPage.deleteView(viewName);
    await manageViewsPage.expectViewNotVisible(viewName);
  });
});
