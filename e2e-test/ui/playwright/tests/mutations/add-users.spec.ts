/**
 * Add users tests — migrated from Cypress e2e/mutations/add_users.js
 *
 * NOTE: The Cypress test is currently skipped (describe.skip) with
 *   // TODO: (v1_ui_removing) migrate this test
 * The tests rely on V1 UI components (invite flow, user list, reset-password)
 * that are being removed. They are kept as skipped here to preserve test
 * intent until the V2 equivalents are built.
 *
 * Tests:
 *   1. Invite a user via signup link, log in with new account
 *   2. Verify reset password link cannot be generated for a non-native user
 *   3. Generate and use a reset password link for a native user
 */

import { test, expect } from '../../fixtures/base-test';

// Shared state across serial tests
let registeredEmail = '';

test.describe.skip('add_user', () => {
  // Tests share the registered user created in test 1
  test.describe.configure({ mode: 'serial' });

  test('go to user link and invite a user', async ({ page }) => {
    const number = Math.floor(Math.random() * 100000);
    const name = `Example Name ${number}`;
    const email = `example${number}@example.com`;

    await page.goto('/settings/identities/users');
    await expect(page.getByText('Invite Users')).toBeVisible({ timeout: 15000 });
    await page.getByText('Invite Users').click();

    // Get the signup invite link from the dialog
    const inviteLinkLocator = page.getByText(/signup\?invite_token=\w{32}/);
    await expect(inviteLinkLocator).toBeVisible({ timeout: 15000 });
    const inviteLink = (await inviteLinkLocator.textContent()) ?? '';

    await page.goto('/settings/identities/users');

    // Log out admin and visit the invite link as new user
    await page.locator('[data-testid="manage-account-menu"]').click();
    await page.locator('[data-testid="log-out-menu-item"]').click({ force: true });
    await expect(page.getByText('Username')).toBeVisible();

    await page.goto(inviteLink);
    await page.locator('[data-testid="email"]').fill(email);
    await page.locator('[data-testid="name"]').fill(name);
    await page.locator('[data-testid="password"]').fill('Example password');
    await page.locator('[data-testid="confirmPassword"]').fill('Example password');
    await page.locator('[data-testid="sign-up"]').click();

    await expect(page.getByText('Welcome back')).toBeVisible({ timeout: 30000 });
    await expect(page.getByText(name)).toBeVisible({ timeout: 15000 });
    registeredEmail = email;

    // Log out and try with a bad token
    await page.locator('[data-testid="manage-account-menu"]').click();
    await page.locator('[data-testid="log-out-menu-item"]').click({ force: true });

    await page.goto('/signup?invite_token=bad_token');
    await page.locator('[data-testid="email"]').fill(email);
    await page.locator('[data-testid="name"]').fill(name);
    await page.locator('[data-testid="password"]').fill('Example password');
    await page.locator('[data-testid="confirmPassword"]').fill('Example password');
    await page.locator('[data-testid="sign-up"]').click();
    await expect(page.getByText('Failed to log in! An unexpected error occurred.')).toBeVisible({ timeout: 15000 });
  });

  test("Verify you can't generate a reset password link for a non-native user", async ({ page }) => {
    await page.goto('/settings/identities/users');
    await expect(page.getByText('Invite Users')).toBeVisible({ timeout: 15000 });
    await page.locator('[data-testid="userItem-non-native"]').first().click();
    await expect(page.locator('[data-testid="reset-menu-item"]')).toHaveAttribute('aria-disabled', 'true');
  });

  test('Generate a reset password link for a native user', async ({ page }) => {
    await page.goto('/settings/identities/users');
    await expect(page.getByText('Invite Users')).toBeVisible({ timeout: 15000 });

    await page
      .locator(`[data-testid="email-native"]`)
      .filter({ hasText: registeredEmail })
      .locator('xpath=ancestor::*[contains(@class,"ant-list-item")]')
      .locator('[data-testid="userItem-native"]')
      .click();

    await page.locator('[data-testid="resetButton"]').first().click();
    await page.locator('[data-testid="refreshButton"]').click();
    await expect(page.getByText('Generated new link to reset credentials')).toBeVisible({ timeout: 15000 });

    await page.locator('.ant-typography-copy').click();
    await page.locator('.ant-modal-close').click();

    const resetLinkLocator = page.getByText(/reset\?reset_token=\w{32}/);
    await expect(resetLinkLocator).toBeVisible({ timeout: 15000 });
    const resetLink = (await resetLinkLocator.textContent()) ?? '';

    // Log out and use the reset link
    await page.locator('[data-testid="manage-account-menu"]').click();
    await page.locator('[data-testid="log-out-menu-item"]').click({ force: true });

    await page.goto(resetLink);
    await page.locator('[data-testid="email"]').fill(registeredEmail);
    await page.locator('[data-testid="password"]').fill('Example Reset Password');
    await page.locator('[data-testid="confirmPassword"]').fill('Example Reset Password');
    await page.locator('[data-testid="reset-password"]').click();
    await expect(page.getByText('Welcome back')).toBeVisible({ timeout: 30000 });

    // Log out and verify bad token fails
    await page.locator('[data-testid="manage-account-menu"]').click();
    await page.locator('[data-testid="log-out-menu-item"]').click({ force: true });

    await page.goto('/reset?reset_token=bad_token');
    await page.locator('[data-testid="email"]').fill(registeredEmail);
    await page.locator('[data-testid="password"]').fill('Example Reset Password');
    await page.locator('[data-testid="confirmPassword"]').fill('Example Reset Password');
    await page.locator('[data-testid="reset-password"]').click();
    await expect(page.getByText('Failed to log in!')).toBeVisible({ timeout: 15000 });
  });
});
