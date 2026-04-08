import { Page, TestInfo } from '@playwright/test';
import { GraphQLHelper } from './graphql-helper';

// ── URL constants ─────────────────────────────────────────────────────────────

const GMS_DELETE_PATH = '/entities?action=delete';

// ── GraphQL mutation constants ────────────────────────────────────────────────

const SOFT_DELETE_MUTATION = `
  mutation batchUpdateSoftDeleted($input: BatchUpdateSoftDeletedInput!) {
    batchUpdateSoftDeleted(input: $input)
  }
`;

const SEARCH_QUERY = `
  query search($input: SearchInput!) {
    search(input: $input) {
      searchResults {
        entity { urn }
      }
    }
  }
`;

const SEARCH_ACROSS_ENTITIES_QUERY = `
  query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {
    searchAcrossEntities(input: $input) {
      searchResults {
        entity { urn }
      }
    }
  }
`;

// ── CleanupHelper ─────────────────────────────────────────────────────────────

/**
 * CleanupHelper — test-scoped cleanup utilities.
 *
 * Use this for deleting specific entities created during a test or a suite.
 * For broad pre-run cleanup of all test data use GlobalCleanupHelper below.
 */
export class CleanupHelper {
  private graphqlHelper: GraphQLHelper;

  constructor(private page: Page) {
    this.graphqlHelper = new GraphQLHelper(page);
  }

  /** Delete entities by URN. Defaults to hard-delete via GMS REST API. */
  async deleteEntities(urns: string[], hardDelete: boolean = true): Promise<void> {
    if (urns.length === 0) return;

    await Promise.all(urns.map((urn) => this.deleteEntity(urn, hardDelete)));
  }

  private async deleteEntity(urn: string, hardDelete: boolean): Promise<void> {
    const baseURL = process.env.BASE_URL || 'http://localhost:9002';
    const gmsUrl = baseURL.replace(':9002', ':8080');

    if (hardDelete) {
      const response = await this.page.request.post(`${gmsUrl}${GMS_DELETE_PATH}`, {
        data: { urn },
        headers: { 'Content-Type': 'application/json' },
      });

      if (!response.ok()) {
        const body = await response.text();
        throw new Error(`Failed to delete ${urn}: ${response.status()} ${body}`);
      }
    } else {
      await this.graphqlHelper.executeQuery(SOFT_DELETE_MUTATION, {
        input: { urns: [urn], deleted: true },
      });
    }
  }

  /**
   * Delete entities of a specific type matching a name pattern.
   *
   * @param entityType - Entity type constant (e.g. 'BUSINESS_ATTRIBUTE', 'TAG')
   * @param namePattern - Required name pattern to filter — prevents accidental
   *   deletion of all entities of that type on the instance.
   */
  async deleteEntitiesByType(entityType: string, namePattern: string): Promise<void> {
    const variables = {
      input: {
        types: [entityType],
        query: namePattern,
        start: 0,
        count: 100,
      },
    };

    const response = await this.graphqlHelper.executeQuery(SEARCH_ACROSS_ENTITIES_QUERY, variables);
    const results = (response?.data?.searchAcrossEntities?.searchResults ?? []) as Array<{
      entity: { urn: string };
    }>;
    const urns = results.map((r) => r.entity.urn);

    if (urns.length > 0) {
      await this.deleteEntities(urns);
    }
  }

  /**
   * Smart cleanup: only delete if the test passed.
   * Preserves entities on failure so failures can be debugged.
   */
  async cleanupOnSuccess(testInfo: TestInfo, urns: string[]): Promise<void> {
    if (testInfo.status === 'passed' || testInfo.status === 'skipped') {
      await this.deleteEntities(urns);
    }
  }
}

// ── GlobalCleanupHelper ───────────────────────────────────────────────────────

/**
 * GlobalCleanupHelper — broad pre-run or post-run cleanup.
 *
 * Deletes ALL test entities matching known patterns.  Use this in global setup
 * scripts, not in individual test files, to prevent accidental data loss.
 */
export class GlobalCleanupHelper {
  private graphqlHelper: GraphQLHelper;

  constructor(private page: Page) {
    this.graphqlHelper = new GraphQLHelper(page);
  }

  /** Delete all entities matching known test-data name patterns. */
  async cleanupAllTestData(): Promise<void> {
    const testPatterns = ['Cypress*', 'Test*', 'test_*', 'cypress_*'];
    const urnsToDelete: string[] = [];

    for (const pattern of testPatterns) {
      const urns = await this.findEntitiesByPattern(pattern);
      urnsToDelete.push(...urns);
    }

    const unique = [...new Set(urnsToDelete)];
    if (unique.length > 0) {
      const cleanup = new CleanupHelper(this.page);
      await cleanup.deleteEntities(unique);
    }
  }

  private async findEntitiesByPattern(pattern: string): Promise<string[]> {
    const variables = {
      input: {
        type: 'DATASET,TAG,GLOSSARY_TERM,GLOSSARY_NODE,BUSINESS_ATTRIBUTE',
        query: pattern,
        start: 0,
        count: 100,
      },
    };

    try {
      const response = await this.graphqlHelper.executeQuery(SEARCH_QUERY, variables);
      const results = (response?.data?.search?.searchResults ?? []) as Array<{
        entity: { urn: string };
      }>;
      return results.map((r) => r.entity.urn);
    } catch {
      return [];
    }
  }
}
