import { TestContext } from 'ember-test-helpers';
import { Server } from 'ember-cli-mirage';

/**
 * Aliases the test context for mirage application / acceptance tests
 */
export type MirageTestContext = TestContext & { server: Server };
