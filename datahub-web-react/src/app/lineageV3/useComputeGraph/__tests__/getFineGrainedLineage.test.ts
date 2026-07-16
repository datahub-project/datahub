import { describe, expect, it } from 'vitest';

import { LineageEntity } from '@app/lineageV3/common';
import { schemaFieldExists } from '@app/lineageV3/useComputeGraph/getFineGrainedLineage';

import { EntityType } from '@types';

describe('schemaFieldExists', () => {
    const createMockNode = (urn: string, schemaFields?: { fieldPath: string }[]): LineageEntity => ({
        id: urn,
        urn,
        type: EntityType.Dataset,
        entity: schemaFields
            ? ({
                  urn,
                  type: EntityType.Dataset,
                  name: 'test',
                  schemaMetadata: {
                      fields: schemaFields,
                  },
              } as any)
            : undefined,
        isExpanded: {} as any,
        fetchStatus: {} as any,
        filters: {} as any,
    });

    it('returns true when field exists with exact match', () => {
        const nodes = new Map([
            [
                'urn:li:dataset:1',
                createMockNode('urn:li:dataset:1', [
                    { fieldPath: 'field1' },
                    { fieldPath: 'field2' },
                    { fieldPath: 'field3' },
                ]),
            ],
        ]);

        expect(schemaFieldExists('urn:li:dataset:1', 'field1', nodes)).toBe(true);
        expect(schemaFieldExists('urn:li:dataset:1', 'field2', nodes)).toBe(true);
        expect(schemaFieldExists('urn:li:dataset:1', 'field3', nodes)).toBe(true);
    });

    it('returns false when field does not exist', () => {
        const nodes = new Map([
            [
                'urn:li:dataset:1',
                createMockNode('urn:li:dataset:1', [{ fieldPath: 'field1' }, { fieldPath: 'field2' }]),
            ],
        ]);

        expect(schemaFieldExists('urn:li:dataset:1', 'nonexistent', nodes)).toBe(false);
        expect(schemaFieldExists('urn:li:dataset:1', 'field3', nodes)).toBe(false);
    });

    it('returns false when dataset does not exist in nodes', () => {
        const nodes = new Map([['urn:li:dataset:1', createMockNode('urn:li:dataset:1', [{ fieldPath: 'field1' }])]]);

        expect(schemaFieldExists('urn:li:dataset:999', 'field1', nodes)).toBe(false);
    });

    it('returns false when node has no entity', () => {
        const nodes = new Map([['urn:li:dataset:1', createMockNode('urn:li:dataset:1')]]);

        expect(schemaFieldExists('urn:li:dataset:1', 'field1', nodes)).toBe(false);
    });

    it('returns false when entity has no schemaMetadata', () => {
        const node: LineageEntity = {
            id: 'urn:li:dataset:1',
            urn: 'urn:li:dataset:1',
            type: EntityType.Dataset,
            entity: {
                urn: 'urn:li:dataset:1',
                type: EntityType.Dataset,
                name: 'test',
            } as any,
            isExpanded: {} as any,
            fetchStatus: {} as any,
            filters: {} as any,
        };
        const nodes = new Map([['urn:li:dataset:1', node]]);

        expect(schemaFieldExists('urn:li:dataset:1', 'field1', nodes)).toBe(false);
    });

    it('returns false when schemaMetadata has no fields', () => {
        const node: LineageEntity = {
            id: 'urn:li:dataset:1',
            urn: 'urn:li:dataset:1',
            type: EntityType.Dataset,
            entity: {
                urn: 'urn:li:dataset:1',
                type: EntityType.Dataset,
                name: 'test',
                schemaMetadata: {} as any,
            } as any,
            isExpanded: {} as any,
            fetchStatus: {} as any,
            filters: {} as any,
        };
        const nodes = new Map([['urn:li:dataset:1', node]]);

        expect(schemaFieldExists('urn:li:dataset:1', 'field1', nodes)).toBe(false);
    });

    it('normalizes V2 field paths for comparison', () => {
        const nodes = new Map([
            [
                'urn:li:dataset:1',
                createMockNode('urn:li:dataset:1', [
                    { fieldPath: '[version=2.0].[type=string].user.id' },
                    { fieldPath: '[version=2.0].[key=True].[type=string].product.name' },
                ]),
            ],
        ]);

        // V1 paths should match V2 paths after normalization
        expect(schemaFieldExists('urn:li:dataset:1', 'user.id', nodes)).toBe(true);
        expect(schemaFieldExists('urn:li:dataset:1', 'product.name', nodes)).toBe(true);
    });

    it('normalizes query field paths with V2 annotations', () => {
        const nodes = new Map([
            ['urn:li:dataset:1', createMockNode('urn:li:dataset:1', [{ fieldPath: 'user.email' }])],
        ]);

        // Query with V2 path should match V1 schema field
        expect(schemaFieldExists('urn:li:dataset:1', '[version=2.0].[type=string].user.email', nodes)).toBe(true);
    });

    it('handles nested field paths', () => {
        const nodes = new Map([
            [
                'urn:li:dataset:1',
                createMockNode('urn:li:dataset:1', [
                    { fieldPath: 'user.profile.name' },
                    { fieldPath: 'order.items.quantity' },
                ]),
            ],
        ]);

        expect(schemaFieldExists('urn:li:dataset:1', 'user.profile.name', nodes)).toBe(true);
        expect(schemaFieldExists('urn:li:dataset:1', 'order.items.quantity', nodes)).toBe(true);
        expect(schemaFieldExists('urn:li:dataset:1', 'user.profile.age', nodes)).toBe(false);
    });

    it('handles empty field list', () => {
        const nodes = new Map([['urn:li:dataset:1', createMockNode('urn:li:dataset:1', [])]]);

        expect(schemaFieldExists('urn:li:dataset:1', 'field1', nodes)).toBe(false);
    });

    it('handles case-sensitive field names', () => {
        const nodes = new Map([['urn:li:dataset:1', createMockNode('urn:li:dataset:1', [{ fieldPath: 'UserId' }])]]);

        expect(schemaFieldExists('urn:li:dataset:1', 'UserId', nodes)).toBe(true);
        expect(schemaFieldExists('urn:li:dataset:1', 'userid', nodes)).toBe(false);
        expect(schemaFieldExists('urn:li:dataset:1', 'USERID', nodes)).toBe(false);
    });
});
