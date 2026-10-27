import { describe, expect, it } from 'vitest';

import buildEntityRegistryV2 from '@app/buildEntityRegistryV2';

import { EntityType } from '@types';

// Lineage treats a node as a ghost when `exists` is falsy. Types that don't fetch `exists`
// must default to true, while an explicit false (a removed entity) must be preserved.
describe('EntityRegistry.getLineageVizConfigV2 exists defaulting', () => {
    const registry = buildEntityRegistryV2();

    it('defaults exists to true when the fetched entity has no exists field', () => {
        const api = { urn: 'urn:li:api:orders.GetOrder', type: EntityType.Api, properties: { name: 'GetOrder' } };

        const node = registry.getLineageVizConfigV2(EntityType.Api, api);

        expect(node?.urn).toBe(api.urn);
        expect(node?.exists).toBe(true);
    });

    it('keeps an explicit exists=false so removed entities stay hidden', () => {
        const api = {
            urn: 'urn:li:api:orders.GetOrder',
            type: EntityType.Api,
            properties: { name: 'GetOrder' },
            exists: false,
        };

        expect(registry.getLineageVizConfigV2(EntityType.Api, api)?.exists).toBe(false);
    });
});
