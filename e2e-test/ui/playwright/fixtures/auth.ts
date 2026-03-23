/**
 * Auth path helpers.
 *
 * This module owns the file-system layout for authentication artefacts.
 * All other modules that need to locate an auth file must import from here
 * rather than constructing paths inline.
 *
 * Files produced by auth.setup.ts:
 *   .auth/{username}.json           — Playwright storageState (browser cookies + localStorage)
 *   .auth/gms-token-{username}.json — GMS personal access token (for REST/GraphQL API calls)
 *
 * NOTE: The old `test` export (UI-login fixture) has been removed. All
 * authentication is now managed by base-test.ts via per-user storageState.
 */

import * as fs from 'fs';
import * as path from 'path';

const AUTH_DIR = path.join(__dirname, '../.auth');

/** Absolute path to the Playwright storageState file for `username`. */
export function authStatePath(username: string): string {
  return path.join(AUTH_DIR, `${username}.json`);
}

/** Absolute path to the GMS token file for `username`. */
export function gmsTokenPath(username: string): string {
  return path.join(AUTH_DIR, `gms-token-${username}.json`);
}

/**
 * Read the GMS personal access token for `username` from disk.
 *
 * Throws a clear error when the file is absent so test authors know to run
 * the auth setup project first.
 */
export function readGmsToken(username: string): string {
  const tokenFile = gmsTokenPath(username);
  if (!fs.existsSync(tokenFile)) {
    throw new Error(
      `GMS token not found for user '${username}'. ` +
        `Run auth setup first: npx playwright test --project=auth-setup`,
    );
  }
  const data = JSON.parse(fs.readFileSync(tokenFile, 'utf-8')) as { token: string };
  return data.token;
}
