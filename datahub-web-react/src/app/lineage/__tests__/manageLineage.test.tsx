import { existsInEntitiesToAdd } from '../manage/AddEntityEdge';

describe('existsInEntitiesToAdd', () => {
    it('should return false if the search result is not in entitiesAlreadyAdded', () => {
        const result = { entity: { urn: 'urn:li:test' } } as any;
        const entitiesAlreadyAdded = [{ urn: 'urn:li:testing123' }] as any;
        const exists = existsInEntitiesToAdd(result, entitiesAlreadyAdded);

        expect(exists).toBe(false);
    });

    it('should return true if the search result is in entitiesAlreadyAdded', () => {
        const result = { entity: { urn: 'urn:li:test' } } as any;
        const entitiesAlreadyAdded = [{ urn: 'urn:li:testing123' }, { urn: 'urn:li:test' }] as any;
        const exists = existsInEntitiesToAdd(result, entitiesAlreadyAdded);

        expect(exists).toBe(true);
    });
});
