import { Page, TestInfo } from '@playwright/test';
import { GraphQLHelper } from './GraphQLHelper';

/**
 * CleanupHelper - Manages test data cleanup with smart strategies
 *
 * Provides utilities to delete entities via GraphQL mutations.
 * Supports smart cleanup (only on success) and global cleanup patterns.
 *
 * @example
 * ```typescript
 * const cleanup = new CleanupHelper(page);
 *
 * // Delete specific URNs
 * await cleanup.deleteEntities(['urn:li:tag:Test', 'urn:li:dataset:...']);
 *
 * // Global cleanup (delete all test entities)
 * await cleanup.cleanupTestData();
 *
 * // Smart cleanup in afterAll hook
 * test.afterAll(async ({ page }) => {
 *   await cleanup.cleanupOnSuccess(test.info(), urns);
 * });
 * ```
 */
export class CleanupHelper {
  private page: Page;
  private graphqlHelper: GraphQLHelper;

  constructor(page: Page) {
    this.page = page;
    this.graphqlHelper = new GraphQLHelper(page);
  }

  /**
   * Delete entities by URN using GraphQL mutation
   *
   * Uses batchUpdateSoftDeleted mutation to mark entities as deleted.
   * For hard deletion, uses the GMS REST API.
   *
   * @param urns - Array of entity URNs to delete
   * @param hardDelete - If true, permanently delete (default: true for tests)
   */
  async deleteEntities(urns: string[], hardDelete: boolean = true): Promise<void> {
    if (urns.length === 0) {
      console.log('ℹ️  No entities to delete');
      return;
    }

    console.log(`🗑️  Deleting ${urns.length} entities...`);

    const deletePromises = urns.map((urn) => this.deleteEntity(urn, hardDelete));

    try {
      await Promise.all(deletePromises);
      console.log(`✅ Successfully deleted ${urns.length} entities`);
    } catch (error) {
      console.error('❌ Failed to delete some entities:', error);
      throw error;
    }
  }

  /**
   * Delete a single entity
   *
   * @param urn - Entity URN
   * @param hardDelete - If true, permanently delete
   */
  private async deleteEntity(urn: string, hardDelete: boolean): Promise<void> {
    const baseURL = process.env.BASE_URL || 'http://localhost:9002';
    const gmsUrl = baseURL.replace(':9002', ':8080');

    if (hardDelete) {
      // Hard delete via GMS REST API
      const response = await this.page.request.post(`${gmsUrl}/entities?action=delete`, {
        data: { urn },
        headers: { 'Content-Type': 'application/json' },
      });

      if (!response.ok()) {
        const body = await response.text();
        throw new Error(`Failed to delete ${urn}: ${response.status()} ${body}`);
      }
    } else {
      // Soft delete via GraphQL
      const mutation = `
        mutation batchUpdateSoftDeleted($input: BatchUpdateSoftDeletedInput!) {
          batchUpdateSoftDeleted(input: $input)
        }
      `;

      const variables = {
        input: {
          urns: [urn],
          deleted: true,
        },
      };

      await this.graphqlHelper.executeQuery(mutation, variables);
    }
  }

  /**
   * Clean up all test data by pattern matching
   *
   * Deletes all entities created by Cypress/Playwright tests.
   * Uses pattern matching on URNs (e.g., Cypress*, Test*, etc.)
   *
   * This is useful for global cleanup before test runs.
   */
  async cleanupTestData(): Promise<void> {
    console.log('🧹 Running global cleanup for test data...');

    const patterns = [
      'Cypress*', // Cypress test entities
      'Test*', // Test entities
      'test_*', // Test entities (snake_case)
      'cypress_*', // Cypress entities (snake_case)
    ];

    const urnsToDelete: string[] = [];

    for (const pattern of patterns) {
      const urns = await this.findEntitiesByPattern(pattern);
      urnsToDelete.push(...urns);
    }

    // Remove duplicates
    const uniqueUrns = [...new Set(urnsToDelete)];

    if (uniqueUrns.length > 0) {
      console.log(`🗑️  Found ${uniqueUrns.length} test entities to clean up`);
      await this.deleteEntities(uniqueUrns);
    } else {
      console.log('✅ No test entities found to clean up');
    }
  }

  /**
   * Find entities matching a pattern
   *
   * Uses search API to find entities by name pattern
   *
   * @param pattern - Search pattern (supports wildcards)
   * @returns Array of matching entity URNs
   */
  private async findEntitiesByPattern(pattern: string): Promise<string[]> {
    const query = `
      query search($input: SearchInput!) {
        search(input: $input) {
          searchResults {
            entity {
              urn
            }
          }
        }
      }
    `;

    const variables = {
      input: {
        type: 'DATASET,TAG,GLOSSARY_TERM,GLOSSARY_NODE,BUSINESS_ATTRIBUTE',
        query: pattern,
        start: 0,
        count: 100,
      },
    };

    try {
      const response = await this.graphqlHelper.executeQuery(query, variables);
      const results = response?.data?.search?.searchResults || [];
      return results.map((result: any) => result.entity.urn);
    } catch (error) {
      console.warn(`⚠️  Failed to search for pattern ${pattern}:`, error);
      return [];
    }
  }

  /**
   * Smart cleanup: only delete entities if test passed
   *
   * Use this in test.afterAll() hooks to preserve test data on failure
   * for debugging purposes.
   *
   * @param testInfo - Playwright TestInfo object
   * @param urns - Entity URNs to delete
   *
   * @example
   * ```typescript
   * test.afterAll(async ({ page }) => {
   *   const cleanup = new CleanupHelper(page);
   *   await cleanup.cleanupOnSuccess(test.info(), global.__BUSINESS_ATTR_URNS);
   * });
   * ```
   */
  async cleanupOnSuccess(testInfo: TestInfo, urns: string[]): Promise<void> {
    if (testInfo.status === 'passed' || testInfo.status === 'skipped') {
      console.log('✅ Test passed/skipped - cleaning up test data');
      await this.deleteEntities(urns);
    } else {
      console.log('⚠️  Test failed - preserving test data for debugging');
      console.log('📝 URNs to manually clean up:', urns);
    }
  }

  /**
   * Delete entities by type
   *
   * Useful for cleaning up specific entity types after tests
   *
   * @param entityType - Entity type (e.g., 'BUSINESS_ATTRIBUTE', 'TAG')
   * @param namePattern - Optional name pattern to filter entities
   */
  async deleteEntitiesByType(entityType: string, namePattern?: string): Promise<void> {
    console.log(`🗑️  Deleting ${entityType} entities${namePattern ? ` matching '${namePattern}'` : ''}...`);

    const query = `
      query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {
        searchAcrossEntities(input: $input) {
          searchResults {
            entity {
              urn
            }
          }
        }
      }
    `;

    const variables = {
      input: {
        types: [entityType],
        query: namePattern || '*',
        start: 0,
        count: 100,
      },
    };

    try {
      const response = await this.graphqlHelper.executeQuery(query, variables);
      const results = response?.data?.searchAcrossEntities?.searchResults || [];
      const urns = results.map((result: any) => result.entity.urn);

      if (urns.length > 0) {
        await this.deleteEntities(urns);
      } else {
        console.log(`ℹ️  No ${entityType} entities found`);
      }
    } catch (error) {
      console.error(`❌ Failed to delete ${entityType} entities:`, error);
      throw error;
    }
  }
}