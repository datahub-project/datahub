/**
 * Incidents V2 tests — migrated from Cypress e2e/incidentsV2/v2_incidents.js
 *
 * Prerequisites (pre-seeded data required):
 *   - Kafka dataset `incidents-sample-dataset-v2` with an existing incident titled "test title"
 *   - BigQuery dataset `cypress_project.jaffle_shop.customers` with sibling datasets
 *
 * ThemeV2 is enabled for every test via apiMock.setFeatureFlags, mirroring
 * the Cypress `cy.setIsThemeV2Enabled(true)` call in beforeEach.
 *
 * Note on test 3 (update & resolve): the Cypress suite shared a single
 * `newIncidentNameWithTimeStamp` variable across tests 2 and 3. In Playwright
 * we generate the timestamp once at describe scope so tests can share it while
 * still remaining order-independent within this file.
 */

import { test, expect } from '../../fixtures/base-test';
import { IncidentsPage } from '../../pages/incidents.page';

test.use({ featureName: 'incidents-v2' });

const EXISTING_INCIDENT_TITLE = 'test title';

const NEW_INCIDENT_VALUES = {
  NAME: 'Incident new name',
  DESCRIPTION: 'This is Description',
  TYPE: 'Freshness',
  PRIORITY: 'Critical',
  STAGE: 'Investigation',
};

const EDITED_INCIDENT_VALUES = {
  DESCRIPTION: 'Edited Description',
  PRIORITY: 'High',
  STAGE: 'In progress',
  TYPE: 'Freshness',
};

// Shared timestamp so that the "create" and "update" tests reference the same incident name.
const sharedTimestamp = Date.now();
const newIncidentName = `${NEW_INCIDENT_VALUES.NAME}-${sharedTimestamp}`;
const editedIncidentName = `${newIncidentName}-edited`;

test.describe('Incidents V2', () => {
  // Tests 2 and 3 share the same incident name (create → update).
  // Serial mode ensures test 2 completes before test 3 starts.
  test.describe.configure({ mode: 'serial' });

  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({
      themeV2Enabled: true,
      themeV2Default: true,
      showNavBarRedesign: true,
    });
  });

  test('can view v1 incident', async ({ page, logger, logDir }) => {
    const incidentsPage = new IncidentsPage(page, logger, logDir);
    await incidentsPage.navigateToKafkaDatasetIncidents();

    // Seeder fixture guarantees the Kafka dataset is present — wait for it to load.
    await expect(page.getByText('Not Found')).not.toBeVisible({ timeout: 20000 });

    const row = incidentsPage.getIncidentRow(EXISTING_INCIDENT_TITLE);
    await expect(row).toBeVisible();
    await expect(row).toContainText(EXISTING_INCIDENT_TITLE);
  });

  test('create a v2 incident with all fields set', async ({ page, logger, logDir }) => {
    const incidentsPage = new IncidentsPage(page, logger, logDir);
    await incidentsPage.navigateToKafkaDatasetIncidents();

    await incidentsPage.clickCreateIncidentBtn();
    await incidentsPage.fillIncidentName(newIncidentName);
    await incidentsPage.fillDescription(NEW_INCIDENT_VALUES.DESCRIPTION);
    await incidentsPage.selectCategory(NEW_INCIDENT_VALUES.TYPE);
    await incidentsPage.selectPriority(NEW_INCIDENT_VALUES.PRIORITY);
    await incidentsPage.selectStage(NEW_INCIDENT_VALUES.STAGE);
    await incidentsPage.selectFirstAssignee();
    await incidentsPage.dismissDropdowns();
    await incidentsPage.submitIncidentForm();

    await incidentsPage.expandGroupIfNeeded('CRITICAL');
    await expect(incidentsPage.getIncidentRow(newIncidentName)).toBeVisible({ timeout: 15000 });
    await incidentsPage.expectIncidentNameBadgeVisible(newIncidentName);
    await incidentsPage.expectIncidentRowStage(newIncidentName, NEW_INCIDENT_VALUES.STAGE);
    await incidentsPage.expectIncidentRowCategory(newIncidentName, NEW_INCIDENT_VALUES.TYPE);
    await incidentsPage.expectResolveButtonVisible(newIncidentName);
    await incidentsPage.expectResolveButtonContains(newIncidentName, 'Resolve');
  });

  test('can update incident & resolve incident', async ({ page, logger, logDir }) => {
    test.setTimeout(120000);
    const incidentsPage = new IncidentsPage(page, logger, logDir);

    // Navigate and wait for the incident created in the previous test.
    // The incidents resolver queries Elasticsearch, which has a reindexing delay after
    // the previous test's mutation.  navigateToKafkaDatasetIncidentsAndWaitForRow retries
    // the navigate → expand cycle until ES catches up (up to ~20 s total).
    await incidentsPage.navigateToKafkaDatasetIncidentsAndWaitForRow(newIncidentName, 'CRITICAL');
    await expect(incidentsPage.getIncidentRow(newIncidentName)).toBeVisible({ timeout: 15000 });
    await incidentsPage.clickIncidentRow(newIncidentName);

    // Edit mode
    await incidentsPage.clickEditIcon();
    await incidentsPage.clearAndFillName(editedIncidentName);
    await incidentsPage.clearAndFillDescription(EDITED_INCIDENT_VALUES.DESCRIPTION);
    await incidentsPage.selectPriority(EDITED_INCIDENT_VALUES.PRIORITY);
    await incidentsPage.selectStage(EDITED_INCIDENT_VALUES.STAGE);
    await incidentsPage.selectFirstAssignee();
    await incidentsPage.dismissDropdowns();
    await incidentsPage.selectStatus('Resolved');
    await incidentsPage.submitIncidentForm();

    // Filter to show RESOLVED incidents
    await incidentsPage.filterByStatus('RESOLVED');

    // Scroll to and verify the HIGH priority group
    const highGroup = incidentsPage.getIncidentGroup('HIGH');
    await highGroup.scrollIntoViewIfNeeded();
    await incidentsPage.expandGroupIfNeeded('HIGH');

    await incidentsPage.getIncidentRow(editedIncidentName).scrollIntoViewIfNeeded();
    await expect(incidentsPage.getIncidentRow(editedIncidentName)).toBeVisible({ timeout: 15000 });
    await incidentsPage.expectIncidentRowStage(editedIncidentName, EDITED_INCIDENT_VALUES.STAGE);
    await incidentsPage.expectIncidentRowCategory(editedIncidentName, EDITED_INCIDENT_VALUES.TYPE);
    await incidentsPage.expectResolveButtonContains(editedIncidentName, 'Me');
  });

  test('Create V2 incident with all fields set and separate_siblings=false', async ({ page, logger, logDir }) => {
    const incidentsPage = new IncidentsPage(page, logger, logDir);
    // is_lineage_mode=false is the default for this navigation helper
    await incidentsPage.navigateToBigQueryDatasetIncidents();

    // The create button shows with siblings variant when separate_siblings is not forced true
    await incidentsPage.clickCreateIncidentBtnWithSiblings();
    await incidentsPage.selectFirstSiblingFromDropdown();

    // Drawer should open for "Create New Incident"
    await incidentsPage.expectDrawerTitleContains('Create New Incident');

    // In sibling mode a second remirror editor may appear (index 1)
    await incidentsPage.fillIncidentName(newIncidentName);
    await incidentsPage.fillDescription(NEW_INCIDENT_VALUES.DESCRIPTION, 1);
    await incidentsPage.selectCategory(NEW_INCIDENT_VALUES.TYPE);
    await incidentsPage.selectPriority(NEW_INCIDENT_VALUES.PRIORITY);
    await incidentsPage.selectStage(NEW_INCIDENT_VALUES.STAGE);
    await incidentsPage.selectFirstAssignee();
    await incidentsPage.dismissDropdowns();
    await incidentsPage.submitIncidentForm();

    await expect(incidentsPage.getIncidentGroup('CRITICAL')).toBeVisible();
    const criticalGroup = incidentsPage.getIncidentGroup('CRITICAL');
    await criticalGroup.scrollIntoViewIfNeeded();
    await incidentsPage.expandGroupIfNeeded('CRITICAL');

    await expect(incidentsPage.getIncidentRow(newIncidentName)).toBeVisible({ timeout: 15000 });
    await incidentsPage.expectIncidentNameBadgeVisible(newIncidentName);
    await incidentsPage.expectIncidentRowStage(newIncidentName, NEW_INCIDENT_VALUES.STAGE);
    await incidentsPage.expectIncidentRowCategory(newIncidentName, NEW_INCIDENT_VALUES.TYPE);
    await incidentsPage.expectResolveButtonVisible(newIncidentName);
    await incidentsPage.expectResolveButtonContains(newIncidentName, 'Resolve');
  });

  test('Create V2 incident with all fields set and separate_siblings=true', async ({ page, logger, logDir }) => {
    const siblingIncidentName = `${newIncidentName}-New`;
    const incidentsPage = new IncidentsPage(page, logger, logDir);
    await incidentsPage.navigateToBigQueryDatasetIncidents('separate_siblings=true');

    await incidentsPage.clickCreateIncidentBtn();
    await incidentsPage.fillIncidentName(siblingIncidentName);
    await incidentsPage.fillDescription(NEW_INCIDENT_VALUES.DESCRIPTION);
    await incidentsPage.selectCategory(NEW_INCIDENT_VALUES.TYPE);
    await incidentsPage.selectPriority(NEW_INCIDENT_VALUES.PRIORITY);
    await incidentsPage.selectStage(NEW_INCIDENT_VALUES.STAGE);
    await incidentsPage.selectFirstAssignee();
    await incidentsPage.dismissDropdowns();
    await incidentsPage.submitIncidentForm();

    await expect(incidentsPage.getIncidentGroup('CRITICAL')).toBeVisible();
    const criticalGroup = incidentsPage.getIncidentGroup('CRITICAL');
    await criticalGroup.scrollIntoViewIfNeeded();
    await incidentsPage.expandGroupIfNeeded('CRITICAL');

    await expect(incidentsPage.getIncidentRow(siblingIncidentName)).toBeVisible({ timeout: 15000 });
    await incidentsPage.expectIncidentNameBadgeVisible(siblingIncidentName);
    await incidentsPage.expectIncidentRowStage(siblingIncidentName, NEW_INCIDENT_VALUES.STAGE);
    await incidentsPage.expectIncidentRowCategory(siblingIncidentName, NEW_INCIDENT_VALUES.TYPE);
    await incidentsPage.expectResolveButtonVisible(siblingIncidentName);
    await incidentsPage.expectResolveButtonContains(siblingIncidentName, 'Resolve');
  });
});
