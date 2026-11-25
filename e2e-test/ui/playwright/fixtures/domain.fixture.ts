/**
 * Domain fixture — writes each test's owning product domains into the JUnit report.
 *
 * A test carries a DomainTag (`@domain:observe`, …; utils/test-tags.ts) on itself. This auto
 * fixture copies those tags into a `domains` annotation, which the JUnit reporter writes as a
 * property (embedAnnotationsAsProperties), so CI results can group failures by domain.
 */
import { test as base } from '@playwright/test';
import { AnnotationType } from '../utils/test-tags';

const DOMAIN_TAG_PREFIX = '@domain:';

export const domainFixture = base.extend<{ _domainAnnotation: void }>({
  _domainAnnotation: [
    async ({}, use, testInfo) => {
      const owners = [
        ...new Set(
          testInfo.tags
            .filter((tag) => tag.startsWith(DOMAIN_TAG_PREFIX))
            .map((tag) => tag.slice(DOMAIN_TAG_PREFIX.length)),
        ),
      ].sort();
      if (owners.length > 0) {
        testInfo.annotations.push({ type: AnnotationType.Domains, description: JSON.stringify(owners) });
      }
      await use();
    },
    { auto: true },
  ],
});
