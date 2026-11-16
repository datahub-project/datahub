/**
 * Guards the raw-identifier guard itself: planted identifiers are reported with their location
 * and in the failure message, and the excluded and hidden regions are not, including a box
 * clipped to zero size the way a select widget's screen-reader list is. Runs on static content,
 * so it uses login-test, the one fixture without auto-login, and never navigates.
 */

import { test, expect } from '../../fixtures/login-test';
import { expectNoRawIdentifiers, findRawIdentifiers } from '../../helpers/raw-identifier-helper';
import { DomainTag } from '../../utils/test-tags';

const PLANTED_PAGE = `
  <main>
    <div data-testid="tag-list"><span>urn:li:tag:7a0b2c1d</span></div>
    <div data-testid="run-id">Run 3f2504e0-4f89-11d3-9a0c-0305e82c3301 finished</div>
    <div data-testid="parent-name">Revenue %28Net%29</div>
    <div data-testid="lower-escape">Revenue %2fNet</div>
    <div data-testid="clean-name">Revenue (Net)</div>
    <pre>urn:li:dataset:(urn:li:dataPlatform:hive,raw,PROD)</pre>
    <button data-testid="share-copy-urn">urn:li:chart:(looker,copyable)</button>
    <div style="display: none">urn:li:hidden:one</div>
    <div aria-hidden="true">urn:li:hidden:two</div>
    <textarea>urn:li:typed:by:user</textarea>
    <div contenteditable="true">urn:li:typed:in:editor</div>
    <div style="height: 0; width: 0; overflow: hidden"><div>urn:li:clipped:screen-reader-list</div></div>
    <div style="opacity: 0">urn:li:hidden:transparent</div>
  </main>
`;

test.describe('raw identifier helper', () => {
  test(
    'reports visible identifiers and ignores excluded or hidden ones',
    { tag: [DomainTag.Catalog] },
    async ({ page }) => {
      await page.setContent(PLANTED_PAGE);

      const findings = await findRawIdentifiers(page);

      expect(findings).toEqual([
        { kind: 'urn', text: 'urn:li:tag:7a0b2c1d', location: 'span in [data-testid="tag-list"]' },
        {
          kind: 'guid',
          text: 'Run 3f2504e0-4f89-11d3-9a0c-0305e82c3301 finished',
          location: 'div in [data-testid="run-id"]',
        },
        { kind: 'url-escape', text: 'Revenue %28Net%29', location: 'div in [data-testid="parent-name"]' },
        { kind: 'url-escape', text: 'Revenue %2fNet', location: 'div in [data-testid="lower-escape"]' },
      ]);

      // The failure message is the deliverable: it must carry the label, kind, text and location.
      await expect(expectNoRawIdentifiers(page, { label: 'planted', settleTimeout: 0 })).rejects.toThrow(
        /planted: raw identifiers rendered as user-visible text:[\s\S]*\[urn\] "urn:li:tag:7a0b2c1d" \(span in \[data-testid="tag-list"\]\)/,
      );
    },
  );

  test('a custom exclusion list replaces the defaults', { tag: [DomainTag.Catalog] }, async ({ page }) => {
    await page.setContent(PLANTED_PAGE);

    const findings = await findRawIdentifiers(page, ['[data-testid="tag-list"]']);

    // The tag list is now ignored, and the <pre> URN is reported because the defaults no longer apply.
    expect(findings.map((finding) => finding.location)).toEqual([
      'div in [data-testid="run-id"]',
      'div in [data-testid="parent-name"]',
      'div in [data-testid="lower-escape"]',
      'pre',
      'button in [data-testid="share-copy-urn"]',
      'textarea',
      'div',
    ]);
  });

  test('passes on a page with only display names', { tag: [DomainTag.Catalog] }, async ({ page }) => {
    await page.setContent(`
      <main>
        <h1 data-testid="entity-name">Customer Orders</h1>
        <span>Owned by Data Platform</span>
        <a href="/dataset/urn%3Ali%3Adataset%3A(x)">Open</a>
      </main>
    `);

    await expectNoRawIdentifiers(page, { label: 'static page' });
  });
});
