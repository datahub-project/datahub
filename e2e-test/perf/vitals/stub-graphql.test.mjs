import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { graphqlStubData, hasGraphqlFixture } from './stub-graphql.mjs';

describe('graphqlStubData', () => {
    it('returns a recorded payload only for own fixture names', () => {
        assert.equal(hasGraphqlFixture('getMe'), true);
        assert.equal(graphqlStubData('getMe').me.corpUser.urn, 'urn:li:corpuser:boot');
        assert.equal(hasGraphqlFixture('toString'), false);
        assert.deepEqual(graphqlStubData('toString'), {});
        assert.deepEqual(graphqlStubData('__proto__'), {});
    });
});
