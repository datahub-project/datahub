import { test, expect } from '../../fixtures/base-test';
import { DomainTag } from '../../utils/test-tags';

test.use({ featureName: 'foreign-key-navigation' });

const ORDERS_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_fk.orders,PROD)';
const CUSTOMERS_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_fk.customers,PROD)';

test(
  'follows a foreign key from a schema field to the referenced dataset',
  { tag: [DomainTag.Catalog] },
  async ({ page }) => {
    await page.goto(`/dataset/${encodeURIComponent(ORDERS_URN)}/Columns`);

    const customerField = page.getByTestId('schema-field-customer_id');
    await customerField.getByRole('button', { name: 'Foreign Key', exact: true }).click();

    const drawer = page.getByTestId('schema-field-drawer-content');
    await expect(drawer.getByText('Foreign Key to', { exact: true })).toBeVisible();
    const constraint = drawer.getByTestId('foreign-key-fk_orders_customers');
    await expect(constraint.getByText('Source fields', { exact: true })).toBeVisible();
    await expect(constraint.getByText('Target fields', { exact: true })).toBeVisible();
    await expect(constraint.getByText('tenant_id', { exact: true })).toHaveCount(2);
    await expect(constraint.getByText('customer_id', { exact: true })).toBeVisible();
    await expect(constraint.getByText('id', { exact: true })).toBeVisible();

    await constraint.getByTestId(`compact-entity-link-${CUSTOMERS_URN}`).click();
    await expect(page).toHaveURL((url) => decodeURIComponent(url.pathname).split('/')[2] === CUSTOMERS_URN);
    await expect(drawer).not.toBeVisible();
    await expect(page.getByText('customers', { exact: true }).first()).toBeVisible();
  },
);
