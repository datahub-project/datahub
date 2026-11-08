import {
    documentTreeRootResetTrigger,
    isCurrentRootLoadGeneration,
    mergeInitializedRootUrns,
} from '@app/document/utils/documentTreeRoots';

describe('mergeInitializedRootUrns', () => {
    it('drops previously paged roots that are not on the new first page', () => {
        const localRoots = new Map<string, 'start' | 'end'>();

        expect(mergeInitializedRootUrns(['a', 'b', 'c'], ['b', 'a'], localRoots)).toEqual(['b', 'a']);
    });

    it('keeps locally added roots ahead of or behind the server page', () => {
        const localRoots = new Map<string, 'start' | 'end'>([
            ['optimistic', 'start'],
            ['reveal-stub', 'end'],
        ]);

        expect(mergeInitializedRootUrns(['optimistic', 'old-page', 'reveal-stub'], ['server'], localRoots)).toEqual([
            'optimistic',
            'server',
            'reveal-stub',
        ]);
    });

    it('does not duplicate a local root once search returns it', () => {
        const localRoots = new Map<string, 'start' | 'end'>([['optimistic', 'start']]);

        expect(mergeInitializedRootUrns(['optimistic', 'old'], ['optimistic', 'server'], localRoots)).toEqual([
            'optimistic',
            'server',
        ]);
    });
});

describe('root load race helpers', () => {
    it('rejects stale or unmounted root-page generations', () => {
        expect(isCurrentRootLoadGeneration(true, 2, 2)).toBe(true);
        expect(isCurrentRootLoadGeneration(true, 2, 1)).toBe(false);
        expect(isCurrentRootLoadGeneration(false, 2, 2)).toBe(false);
    });

    it('includes View in the infinite-scroll reset key', () => {
        expect(documentTreeRootResetTrigger('name_asc', 'urn:li:dataHubView:a')).toBe('name_asc:urn:li:dataHubView:a');
        expect(documentTreeRootResetTrigger('name_asc', null)).toBe('name_asc:');
    });
});
