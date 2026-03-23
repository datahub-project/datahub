/**
 * User credential registry — the single source of truth for all test users.
 *
 * To add a new user, add an entry here. The `role` field is reserved for
 * role-based fixture expansion; add new roles to the union type as needed.
 *
 * CI override: create a sibling file `users.ci.ts` (do NOT commit it) that
 * exports a `users` object with the same shape. Any keys present there will
 * override the entries below at runtime. CI pipelines set credentials via
 * that file (or an environment-specific import) without touching this file.
 *
 * Example users.ci.ts:
 *   import type { UserMap } from './users';
 *   export const users: UserMap = {
 *     admin: { username: 'ci_admin', password: process.env.ADMIN_PASS!, role: 'admin' },
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
    username: 'datahub',
    password: 'datahub',
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
  // No CI overrides — using local defaults.
}

/**
 * Resolved user registry: local defaults merged with any CI overrides.
 * Always import credentials from here, never from `users` directly.
 */
export const resolvedUsers: UserMap = { ...users, ...ciOverrides };
