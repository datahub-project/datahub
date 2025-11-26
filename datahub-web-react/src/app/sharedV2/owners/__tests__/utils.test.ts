import { describe, expect, it } from 'vitest';

import { getOwnersChanges } from '@app/sharedV2/owners/utils';

import { EntityType } from '@types';

describe('getOwnersChanges', () => {
    const mockNewOwner1: any = {
        urn: 'urn:li:corpuser:new1',
        type: 'CORP_USER',
    };

    const mockNewOwner2: any = {
        urn: 'urn:li:corpuser:new2',
        type: 'CORP_USER',
    };

    const mockExistingOwner1: any = {
        owner: {
            urn: 'urn:li:corpuser:existing1',
        },
    };

    const mockExistingOwner2: any = {
        owner: {
            urn: 'urn:li:corpuser:existing2',
        },
    };

    it('should return empty arrays when both inputs are undefined', () => {
        const result = getOwnersChanges(undefined, undefined);

        expect(result.ownersToAdd).toEqual([]);
        expect(result.ownersToRemove).toEqual([]);
    });

    it('should return empty arrays when both inputs are empty arrays', () => {
        const result = getOwnersChanges([], []);

        expect(result.ownersToAdd).toEqual([]);
        expect(result.ownersToRemove).toEqual([]);
    });

    it('should identify owners to add when there are new owners but no existing ones', () => {
        const result = getOwnersChanges([mockNewOwner1, mockNewOwner2], []);

        expect(result.ownersToAdd).toEqual([mockNewOwner1, mockNewOwner2]);
        expect(result.ownersToRemove).toEqual([]);
    });

    it('should identify owners to remove when there are existing owners but no new ones', () => {
        const result = getOwnersChanges([], [mockExistingOwner1, mockExistingOwner2]);

        expect(result.ownersToAdd).toEqual([]);
        expect(result.ownersToRemove).toEqual([mockExistingOwner1, mockExistingOwner2]);
    });

    it('should return all new owners when existing owners are undefined', () => {
        const result = getOwnersChanges([mockNewOwner1], undefined);

        expect(result.ownersToAdd).toEqual([mockNewOwner1]);
        expect(result.ownersToRemove).toEqual([]);
    });

    it('should return all existing owners when new owners are undefined', () => {
        const result = getOwnersChanges(undefined, [mockExistingOwner1]);

        expect(result.ownersToAdd).toEqual([]);
        expect(result.ownersToRemove).toEqual([mockExistingOwner1]);
    });

    it('should identify both additions and removals correctly', () => {
        const mockExisting = [
            {
                owner: { urn: 'urn:li:corpuser:A', username: 'A', type: EntityType.CorpUser },
                associatedUrn: 'test',
            },
            {
                owner: { urn: 'urn:li:corpuser:B', username: 'B', type: EntityType.CorpUser },
                associatedUrn: 'test',
            },
        ];
        const mockNew = [
            { urn: 'urn:li:corpuser:B', type: EntityType.CorpUser },
            { urn: 'urn:li:corpuser:C', type: EntityType.CorpUser },
        ];

        const result = getOwnersChanges(mockNew, mockExisting);

        expect(result.ownersToAdd).toEqual([{ urn: 'urn:li:corpuser:C' }]);
        expect(result.ownersToRemove).toEqual([{ owner: { urn: 'urn:li:corpuser:A' } }]);
    });

    it('should handle identical arrays with no changes', () => {
        const mockExisting = [
            { owner: { urn: 'urn:li:corpuser:A', type: EntityType.CorpUser, username: 'A' }, associatedUrn: 'test' },
            { owner: { urn: 'urn:li:corpuser:B', type: EntityType.CorpUser, username: 'B' }, associatedUrn: 'test' },
        ];
        const mockNew = [
            { urn: 'urn:li:corpuser:A', type: EntityType.CorpUser },
            { urn: 'urn:li:corpuser:B', type: EntityType.CorpUser },
        ];

        const result = getOwnersChanges(mockNew, mockExisting);

        expect(result.ownersToAdd).toEqual([]);
        expect(result.ownersToRemove).toEqual([]);
    });
});
