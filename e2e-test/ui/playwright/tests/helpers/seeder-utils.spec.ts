/**
 * Unit checks for the seed settle wait's lag reader. No browser and no DataHub: the
 * request context is a stub that returns canned lag envelopes.
 */

import { test, expect, type APIRequestContext } from '@playwright/test';
import { totalConsumerLag } from '../../helpers/seeder-utils';
import { DomainTag } from '../../utils/test-tags';

function stubApi(body: unknown): APIRequestContext {
  const response = { ok: () => true, json: async () => body };
  return { get: async () => response } as unknown as APIRequestContext;
}

const lagEnvelope = (totalLag: unknown) => ({
  consumerGroups: { 'generic-mae-consumer-job-client': { MetadataChangeLog_Versioned_v1: { metrics: { totalLag } } } },
});

test.describe('totalConsumerLag', () => {
  test('sums lag across the consumer endpoints', { tag: [DomainTag.PlatformInternal] }, async () => {
    expect(await totalConsumerLag(stubApi(lagEnvelope(2)), 'http://gms')).toBe(6);
  });

  test('treats an envelope without consumer groups as unknown', { tag: [DomainTag.PlatformInternal] }, async () => {
    expect(await totalConsumerLag(stubApi({ consumerGroups: {} }), 'http://gms')).toBeNull();
    expect(await totalConsumerLag(stubApi({}), 'http://gms')).toBeNull();
  });

  test('treats a group with no topics as unknown', { tag: [DomainTag.PlatformInternal] }, async () => {
    expect(await totalConsumerLag(stubApi({ consumerGroups: { group: {} } }), 'http://gms')).toBeNull();
  });

  test('treats missing lag metrics as unknown', { tag: [DomainTag.PlatformInternal] }, async () => {
    expect(await totalConsumerLag(stubApi(lagEnvelope(null)), 'http://gms')).toBeNull();
    expect(
      await totalConsumerLag(stubApi({ consumerGroups: { group: { topic: { metrics: null } } } }), 'http://gms'),
    ).toBeNull();
  });
});
