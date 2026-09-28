import { mergeUrnEntities, prependMissingUrnEntities } from '@app/sharedV2/utils/mergeUrnEntities';

type TestEntity = {
    urn: string;
    name: string;
};

describe('mergeUrnEntities', () => {
    it('replaces the list on the first page', () => {
        const current: TestEntity[] = [{ urn: 'old', name: 'Old' }];
        const fresh: TestEntity[] = [{ urn: 'new', name: 'New' }];

        expect(mergeUrnEntities(current, fresh, true)).toBe(fresh);
    });

    it('updates duplicates and appends new rows in server order', () => {
        const current: TestEntity[] = [
            { urn: 'a', name: 'A' },
            { urn: 'b', name: 'Old B' },
        ];
        const updatedB = { urn: 'b', name: 'New B' };
        const addedC = { urn: 'c', name: 'C' };

        expect(mergeUrnEntities(current, [updatedB, addedC], false)).toEqual([current[0], updatedB, addedC]);
    });

    it('keeps later-page lists unique when a page repeats a URN', () => {
        const current: TestEntity[] = [{ urn: 'a', name: 'A' }];
        const firstB = { urn: 'b', name: 'B1' };
        const secondB = { urn: 'b', name: 'B2' };

        expect(mergeUrnEntities(current, [firstB, secondB], false)).toEqual([current[0], firstB]);
    });
});

describe('prependMissingUrnEntities', () => {
    it('prepends only entities that are not already in the list', () => {
        const current: TestEntity[] = [{ urn: 'a', name: 'A' }];
        const extras: TestEntity[] = [
            { urn: 'selected', name: 'Selected' },
            { urn: 'a', name: 'Duplicate A' },
        ];

        expect(prependMissingUrnEntities(current, extras)).toEqual([extras[0], current[0]]);
    });
});
