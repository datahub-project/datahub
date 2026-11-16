/**
 * Raw identifier guard: fails when an identifier leaks into user-visible text.
 *
 * Several escalations shared one symptom with different causes: a modal listed tag URN tails
 * instead of tag names, a drawer showed a URL-encoded name, a settings page showed the URNs of
 * deleted agents. The frontend's display-name helper falls back to the URN whenever the name is
 * missing, so a slip in a GraphQL fragment, a mapper, a resolver or a component all produce the
 * same screen. Scanning for that screen covers every one of them, including surfaces nobody wrote
 * a per-feature assertion for.
 *
 * Usage, after a page renders or a modal opens:
 *
 *   await expectNoRawIdentifiers(page, { label: 'dataset profile' });
 *
 * Known gaps, so the guard is not read as covering more than it does:
 *   - an identifier tail rendered without its prefix (a tag key such as `pii_7a0b` reads as a
 *     plausible name) is only caught when the tail is a GUID;
 *   - an identifier split across adjacent inline elements is not matched, since text nodes are
 *     tested one at a time;
 *   - attributes (`title`, `placeholder`, `href`) are not scanned;
 *   - documentation prose that mentions a URN on purpose will trip it; pass an exclusion for that
 *     region rather than loosening the patterns.
 */

import { expect, type Page } from '@playwright/test';
import { TIMEOUTS } from '../utils/constants';

export type RawIdentifierKind = 'urn' | 'guid' | 'url-escape';

export interface RawIdentifierFinding {
  kind: RawIdentifierKind;
  /** The offending text, trimmed to 120 characters. */
  text: string;
  /** Element tag plus the nearest data-testid ancestor, so the screenshot can be matched to it. */
  location: string;
}

/** Shapes a user should never see as-is. Sources, not RegExp objects, so they cross into the browser. */
const PATTERN_SOURCES: Record<RawIdentifierKind, string> = {
  urn: 'urn:li:',
  guid: '\\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\b',
  'url-escape': '%[0-9A-F]{2}',
};

/** Case-insensitive except the URN prefix, which is always lower case. */
const PATTERN_FLAGS: Record<RawIdentifierKind, string> = { urn: '', guid: 'i', 'url-escape': 'i' };

/**
 * Places that show identifiers on purpose: copy-URN controls, raw and source views, free-form
 * property values, and text the user typed themselves (a text area or an open rich-text editor).
 */
export const DEFAULT_RAW_IDENTIFIER_EXCLUSIONS: readonly string[] = [
  '[data-testid*="copy-urn"]',
  '[data-testid="schema-raw-view"]',
  '[data-testid="entity-properties-table"]',
  'pre',
  'code',
  'textarea',
  '[contenteditable="true"]',
];

const MAX_REPORTED_FINDINGS = 10;
const SETTLE_POLL_INTERVAL_MS = 250;

export interface RawIdentifierOptions {
  /** Selectors to ignore; replaces the defaults, so spread DEFAULT_RAW_IDENTIFIER_EXCLUSIONS to extend them. */
  exclude?: readonly string[];
  /** Prefix for the failure message, e.g. the page or modal being checked. */
  label?: string;
  /** How long a finding may persist before it counts; see expectNoRawIdentifiers. */
  settleTimeout?: number;
}

/** Collect visible text nodes that match a raw-identifier shape, outside the excluded regions. */
export async function findRawIdentifiers(
  page: Page,
  exclude: readonly string[] = DEFAULT_RAW_IDENTIFIER_EXCLUSIONS,
): Promise<RawIdentifierFinding[]> {
  return page.evaluate(
    ({ excludeSelector, patternSources, patternFlags }) => {
      const patterns = Object.entries(patternSources).map(([kind, source]) => ({
        kind: kind as RawIdentifierKind,
        regex: new RegExp(source, patternFlags[kind as RawIdentifierKind]),
      }));
      // Text inside an ancestor that is clipped to nothing is not on screen even though no
      // element in the chain is display:none. Select widgets keep such a box for screen readers
      // and fill it with option values, which for entity pickers are URNs.
      const isClippedAway = (element: Element): boolean => {
        for (let node: Element | null = element; node && node !== document.body; node = node.parentElement) {
          const rect = node.getBoundingClientRect();
          if (rect.width > 0 && rect.height > 0) continue;
          const style = getComputedStyle(node);
          if (style.overflow === 'hidden' || style.overflowX === 'hidden' || style.overflowY === 'hidden') return true;
        }
        return false;
      };
      const findings: RawIdentifierFinding[] = [];
      const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
      while (walker.nextNode()) {
        const text = walker.currentNode.textContent?.trim();
        const element = walker.currentNode.parentElement;
        if (!text || !element) continue;
        if (element.closest('script, style, noscript, [aria-hidden="true"]')) continue;
        if (excludeSelector && element.closest(excludeSelector)) continue;
        if (!element.checkVisibility({ checkVisibilityCSS: true, checkOpacity: true })) continue;
        if (isClippedAway(element)) continue;
        const match = patterns.find((pattern) => pattern.regex.test(text));
        if (!match) continue;
        const testId = element.closest('[data-testid]')?.getAttribute('data-testid');
        findings.push({
          kind: match.kind,
          text: text.slice(0, 120),
          location: `${element.tagName.toLowerCase()}${testId ? ` in [data-testid="${testId}"]` : ''}`,
        });
      }
      return findings;
    },
    { excludeSelector: exclude.join(', '), patternSources: PATTERN_SOURCES, patternFlags: PATTERN_FLAGS },
  );
}

/**
 * Fail the test if a raw identifier stays visible on the page.
 *
 * A profile hydrates over several queries and the display-name fallback can show a URN for a
 * moment before the name arrives. That flash is not what this guards against, so the scan is
 * repeated until the page is clean or `settleTimeout` (default TIMEOUTS.SHORT) elapses, and the
 * last scan's findings are reported.
 */
export async function expectNoRawIdentifiers(page: Page, options: RawIdentifierOptions = {}): Promise<void> {
  const deadline = Date.now() + (options.settleTimeout ?? TIMEOUTS.SHORT);
  let findings = await findRawIdentifiers(page, options.exclude);
  while (findings.length > 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, SETTLE_POLL_INTERVAL_MS));
    findings = await findRawIdentifiers(page, options.exclude);
  }
  const lines = findings
    .slice(0, MAX_REPORTED_FINDINGS)
    .map((finding) => `  [${finding.kind}] "${finding.text}" (${finding.location})`);
  if (findings.length > MAX_REPORTED_FINDINGS) {
    lines.push(`  ...and ${findings.length - MAX_REPORTED_FINDINGS} more`);
  }
  const label = options.label ? `${options.label}: ` : '';
  expect(findings, `${label}raw identifiers rendered as user-visible text:\n${lines.join('\n')}`).toEqual([]);
}
