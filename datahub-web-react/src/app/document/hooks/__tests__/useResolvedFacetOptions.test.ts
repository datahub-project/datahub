import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useResolvedFacetOptions } from '@app/document/hooks/useResolvedFacetOptions';
import { renderHook } from '@utils/test-utils/renderHook';

import { EntityType } from '@types';

const mocks = vi.hoisted(() => ({
    requestedUrns: [] as string[][],
    entities: [] as any[],
}));

vi.mock('@app/sharedV2/useGetEntities', () => ({
    useGetEntities: (urns: string[]) => {
        mocks.requestedUrns.push(urns);
        return { entities: mocks.entities.filter((entity) => urns.includes(entity.urn)), loading: false };
    },
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({ getDisplayName: (_type: EntityType, entity: any) => entity.properties?.name }),
}));

const FINANCE = { urn: 'urn:li:domain:finance', type: EntityType.Domain, properties: { name: 'Finance' } };
const MARKETING = { urn: 'urn:li:domain:marketing', type: EntityType.Domain, properties: { name: 'Marketing' } };
const financeOption = { value: FINANCE.urn, label: 'Finance', entity: FINANCE as any };

describe('useResolvedFacetOptions', () => {
    beforeEach(() => {
        mocks.requestedUrns = [];
        mocks.entities = [];
    });

    it('reuses a name from an earlier facet response without requesting it', () => {
        const { result, rerender } = renderHook(
            ({ urns, facetOptions }) => useResolvedFacetOptions(urns, facetOptions),
            {
                initialProps: { urns: [] as string[], facetOptions: { domainOptions: [financeOption] } },
            },
        );
        // A search drops Finance from the facet while it stays selected.
        rerender({ urns: [FINANCE.urn], facetOptions: { domainOptions: [] } });

        expect(result.current.get(FINANCE.urn)?.label).toBe('Finance');
        expect(mocks.requestedUrns.every((urns) => urns.length === 0)).toBe(true);
    });

    it('looks up only urns never seen in a facet response', () => {
        mocks.entities = [MARKETING];
        const { result, rerender } = renderHook(
            ({ urns, facetOptions }) => useResolvedFacetOptions(urns, facetOptions),
            {
                initialProps: { urns: [] as string[], facetOptions: { domainOptions: [financeOption] } },
            },
        );
        rerender({ urns: [FINANCE.urn, MARKETING.urn], facetOptions: { domainOptions: [] } });

        expect(mocks.requestedUrns[mocks.requestedUrns.length - 1]).toEqual([MARKETING.urn]);
        expect(result.current.get(FINANCE.urn)?.label).toBe('Finance');
        expect(result.current.get(MARKETING.urn)?.label).toBe('Marketing');
    });
});
