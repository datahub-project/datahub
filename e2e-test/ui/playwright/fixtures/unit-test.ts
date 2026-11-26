/**
 * Unit test fixture — for specs that check the test framework's own helpers
 * (seeders, waits, parsers) against stubs, with no browser and no DataHub.
 *
 * Composes only the fixtures that need nothing running:
 *
 *   loggerFixture — Winston structured logging, auto-injected into every test
 *   domainFixture — writes the test's DomainTag into the JUnit report
 *
 * base-test is not usable here: its worker-scoped seeding fixture logs in through
 * a browser, mints a GMS token and ingests the global seed data before any test
 * runs, so a stub-only check would fail whenever DataHub is unreachable.
 *
 * Usage:
 *
 *   import { test, expect } from '../../fixtures/unit-test';
 */

import { mergeTests } from '@playwright/test';
import { loggerFixture } from './logger.fixture';
import { domainFixture } from './domain.fixture';

export const test = mergeTests(loggerFixture, domainFixture);

export { expect } from '@playwright/test';
