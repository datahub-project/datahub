/**
 * User credential registry — the single source of truth for all test users.
 *
 * Credentials are read from environment variables so no secrets are hardcoded.
 * Set these in your local shell or CI environment:
 *
 *   DATAHUB_ADMIN_USERNAME   (default: "datahub")
 *   DATAHUB_ADMIN_PASSWORD   (default: "datahub")
 *
 * CI override: create a sibling file `users.ci.ts` (do NOT commit it) that
 * exports a `users` object with the same shape. Any keys present there will
 * override the entries below at runtime.
 *
 * Example users.ci.ts:
 *   import type { UserMap } from './users';
 *   export const users: UserMap = {
 *     admin: { username: process.env.ADMIN_USERNAME!, password: process.env.ADMIN_PASSWORD!, role: 'admin' },
 *   };
 */

export interface UserCredentials {
  username: string;
  password: string;
  displayName?: string;
  role: 'admin' | 'reader' | 'editor';
}

/** Strongly-typed map shape so CI overrides are validated at compile time. */
export type UserMap = Record<string, UserCredentials>;

export const users = {
  admin: {
    username: process.env.DATAHUB_ADMIN_USERNAME ?? 'datahub',
    password: process.env.DATAHUB_ADMIN_PASSWORD ?? 'datahub',
    displayName: 'DataHub Admin',
    role: 'admin' as const,
  },
} satisfies UserMap;

// Load CI overrides from an uncommitted sibling file when present.
// The try/catch is intentional: absence of the file is the normal case locally.
let ciOverrides: UserMap = {};
try {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  ciOverrides = (require('./users.ci') as { users: UserMap }).users;
} catch {
  // No CI overrides — using defaults.
}

/**
 * Resolved user registry: env-var defaults merged with any CI overrides.
 * Always import credentials from here, never from `users` directly.
 */
export const resolvedUsers: UserMap = { ...users, ...ciOverrides };
