/**
 * Auth path helpers.
 *
 * This module owns the file-system layout for authentication artefacts.
 * All other modules that need to locate an auth file must import from here
 * rather than constructing paths inline.
 *
 * Files written by loginFixture (login.fixture.ts) on first use:
 *   .auth/{username}.json           — Playwright storageState (browser cookies + localStorage)
 *   .auth/gms-token-{username}.json — GMS personal access token (for REST/GraphQL API calls)
 *
 * There is NO separate auth-setup project required. loginFixture handles
 * authentication lazily on the first test run per worker and caches the
 * results to .auth/. Subsequent tests reuse the cached session directly.
 * auth.setup.ts exists only for manual pre-population of the .auth/ files
 * and is intentionally excluded from the default project via testIgnore.
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
        `Run at least one test first so the login fixture can create the token, ` +
        `or delete .auth/ and re-run to force a fresh login.`,
    );
  }
  const data = JSON.parse(fs.readFileSync(tokenFile, 'utf-8')) as { token: string };
  return data.token;
}
