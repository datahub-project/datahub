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
    const foreignKey = customerField.getByRole('button', { name: 'Foreign Key', exact: true });
    const drawer = page.getByTestId('schema-field-drawer-content');
    const statisticsTab = drawer.getByRole('tab', { name: 'Statistics' });
    const idField = page.getByTestId('schema-field-id');

    await foreignKey.click();
    await statisticsTab.click();
    await idField.click();
    await expect(drawer.getByText('id', { exact: true }).first()).toBeVisible();
    await expect(statisticsTab).toHaveAttribute('aria-selected', 'true');
    await idField.click();
    await expect(drawer).not.toBeVisible();

    await foreignKey.click();
    await expect(drawer.getByRole('tab', { name: 'About' })).toHaveAttribute('aria-selected', 'true');
    await expect(drawer.getByText('Foreign Key to', { exact: true })).toBeVisible();
    const constraint = drawer.getByTestId('foreign-key-fk_orders_customers');
    const rows = constraint.getByRole('row');
    await expect(rows).toHaveCount(3);
    await expect(rows.nth(0).getByRole('columnheader')).toHaveText(['Source fields', 'Target fields']);
    await expect(rows.nth(1).getByRole('cell')).toHaveText(['tenant_id', 'tenant_id']);
    await expect(rows.nth(2).getByRole('cell')).toHaveText(['customer_id', 'id']);

    await constraint.getByTestId(`compact-entity-link-${CUSTOMERS_URN}`).click();
    await expect(page).toHaveURL((url) => decodeURIComponent(url.pathname).split('/')[2] === CUSTOMERS_URN);
    await expect(drawer).not.toBeVisible();
    await expect(page.getByText('customers', { exact: true }).first()).toBeVisible();
  },
);
