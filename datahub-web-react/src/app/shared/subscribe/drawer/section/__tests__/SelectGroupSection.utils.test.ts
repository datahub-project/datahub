import { describe, expect, test } from 'vitest';

import {
    convertGroupRelationshipToOption,
    getGroupOptions,
} from '@app/shared/subscribe/drawer/section/SelectGroupSection.utils';

import { EntityType, RelationshipDirection } from '@types';

// Mock data
const mockEntityRegistry = {
    getDisplayName: (_, entity: any) => {
        return entity.properties?.displayName || entity.info?.displayName || entity.name || `Group ${entity.urn}`;
    },
};

const mockCorpGroup1 = {
    urn: 'urn:li:corpGroup:group1',
    type: EntityType.CorpGroup,
    name: 'Engineering Team',
    properties: {
        displayName: 'Engineering Team',
    },
    info: {
        displayName: 'Engineering Team',
    },
};

const mockCorpGroup2 = {
    urn: 'urn:li:corpGroup:group2',
    type: EntityType.CorpGroup,
    name: 'Data Science Team',
    properties: {
        displayName: 'Data Science Team',
    },
    info: {
        displayName: 'Data Science Team',
    },
};

const mockCorpGroup3 = {
    urn: 'urn:li:corpGroup:group3',
    type: EntityType.CorpGroup,
    name: 'Product Team',
};

describe('SelectGroupSection.utils', () => {
    describe('convertGroupRelationshipToOption', () => {
        test('should convert a group to an option with correct label and value', () => {
            const result = convertGroupRelationshipToOption(mockCorpGroup1, mockEntityRegistry as any);

            expect(result).toEqual({
                label: 'Engineering Team',
                value: 'urn:li:corpGroup:group1',
            });
        });

        test('should handle group with fallback name', () => {
            const result = convertGroupRelationshipToOption(mockCorpGroup3, mockEntityRegistry as any);

            expect(result).toEqual({
                label: 'Product Team',
                value: 'urn:li:corpGroup:group3',
            });
        });

        test('should handle group with undefined urn', () => {
            const groupWithoutUrn = { ...mockCorpGroup1, urn: undefined as any };
            const result = convertGroupRelationshipToOption(groupWithoutUrn, mockEntityRegistry as any);

            expect(result).toEqual({
                label: 'Engineering Team',
                value: undefined,
            });
        });
    });

    describe('getGroupOptions', () => {
        test('should return options from both relationships and owned groups', () => {
            const relationships = [
                { entity: mockCorpGroup1, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' },
                { entity: mockCorpGroup2, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' },
            ];

            const ownedGroups = [{ entity: mockCorpGroup3 }];

            const result = getGroupOptions(relationships, ownedGroups, mockEntityRegistry as any);

            expect(result).toHaveLength(3);
            expect(result).toEqual([
                { label: 'Engineering Team', value: 'urn:li:corpGroup:group1' },
                { label: 'Data Science Team', value: 'urn:li:corpGroup:group2' },
                { label: 'Product Team', value: 'urn:li:corpGroup:group3' },
            ]);
        });

        test('should deduplicate groups with same URN', () => {
            const relationships = [
                { entity: mockCorpGroup1, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' },
                { entity: mockCorpGroup2, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' },
            ];

            const ownedGroups = [
                { entity: mockCorpGroup1 }, // Duplicate of group1
                { entity: mockCorpGroup3 },
            ];

            const result = getGroupOptions(relationships, ownedGroups, mockEntityRegistry as any);

            expect(result).toHaveLength(3);
            expect(result.filter((option) => option.value === 'urn:li:corpGroup:group1')).toHaveLength(1);
        });

        test('should handle only relationships with no owned groups', () => {
            const relationships = [
                { entity: mockCorpGroup1, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' },
                { entity: mockCorpGroup2, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' },
            ];

            const result = getGroupOptions(relationships, undefined, mockEntityRegistry as any);

            expect(result).toHaveLength(2);
            expect(result).toEqual([
                { label: 'Engineering Team', value: 'urn:li:corpGroup:group1' },
                { label: 'Data Science Team', value: 'urn:li:corpGroup:group2' },
            ]);
        });

        test('should handle only owned groups with no relationships', () => {
            const ownedGroups = [{ entity: mockCorpGroup1 }, { entity: mockCorpGroup3 }];

            const result = getGroupOptions(undefined, ownedGroups, mockEntityRegistry as any);

            expect(result).toHaveLength(2);
            expect(result).toEqual([
                { label: 'Engineering Team', value: 'urn:li:corpGroup:group1' },
                { label: 'Product Team', value: 'urn:li:corpGroup:group3' },
            ]);
        });

        test('should return empty array when both inputs are undefined', () => {
            const result = getGroupOptions(undefined, undefined, mockEntityRegistry as any);

            expect(result).toEqual([]);
        });

        test('should return empty array when both inputs are empty', () => {
            const result = getGroupOptions([], [], mockEntityRegistry as any);

            expect(result).toEqual([]);
        });

        test('should filter out relationships with null/undefined entities', () => {
            const relationships = [
                { entity: mockCorpGroup1, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' },
                { entity: null, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' },
                { entity: undefined, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' },
                { entity: mockCorpGroup2, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' },
            ];

            const result = getGroupOptions(relationships, undefined, mockEntityRegistry as any);

            expect(result).toHaveLength(2);
            expect(result).toEqual([
                { label: 'Engineering Team', value: 'urn:li:corpGroup:group1' },
                { label: 'Data Science Team', value: 'urn:li:corpGroup:group2' },
            ]);
        });

        test('should handle complex deduplication scenario', () => {
            const relationships = [
                { entity: mockCorpGroup1, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' },
                { entity: mockCorpGroup2, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' },
                { entity: mockCorpGroup1, direction: RelationshipDirection.Outgoing, type: 'IsPartOf' }, // Duplicate within relationships
            ];

            const ownedGroups = [
                { entity: mockCorpGroup2 }, // Duplicate with relationships
                { entity: mockCorpGroup3 },
                { entity: mockCorpGroup3 }, // Duplicate within owned groups
            ];

            const result = getGroupOptions(relationships, ownedGroups, mockEntityRegistry as any);

            expect(result).toHaveLength(3);
            expect(result).toEqual([
                { label: 'Engineering Team', value: 'urn:li:corpGroup:group1' },
                { label: 'Data Science Team', value: 'urn:li:corpGroup:group2' },
                { label: 'Product Team', value: 'urn:li:corpGroup:group3' },
            ]);
        });
    });
});
