/**
 * Tag and annotation vocabulary for Playwright tests.
 * Single source of truth so every test file references the same values
 * instead of repeating string literals.
 *
 *   test('create a secret', { tag: [TestTag.P0, DomainTag.Ingestion] }, async () => { ... });
 */

/** Playwright test tags. Must start with '@' — Playwright only recognizes tags with that prefix for --grep/--grep-invert. */
export enum TestTag {
  /**
   * Critical path: the tests to run first, e.g. `--grep @p0`. Inside a `mode: 'serial'` describe,
   * tag the describe, never a single test, so a P0 run never runs half a serial chain (enforced by
   * .github/scripts/check_playwright_p0.py).
   */
  P0 = '@p0',
}

/**
 * Owning product domain, e.g. `--grep @domain:observe`. fixtures/domain.fixture.ts writes it into
 * the JUnit report.
 */
export enum DomainTag {
  Platform = '@domain:platform',
  Observe = '@domain:observe',
  Ingestion = '@domain:ingestion',
  AI = '@domain:ai',
  Catalog = '@domain:catalog',
  PlatformInternal = '@domain:platform_internal',
}

/** `annotation.type` values for test metadata (not --grep-filterable). */
export enum AnnotationType {
  /**
   * JSON array of owning domains, added by fixtures/domain.fixture.ts — never set by hand. The
   * JUnit reporter writes it as a `domains` property, so CI results can group failures by domain.
   */
  Domains = 'domains',
}
