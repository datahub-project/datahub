import { describe, expect, it } from 'vitest';

import { LineageEntity, createEdgeId } from '@app/lineageV3/common';
import getFineGrainedLineage, { schemaFieldExists } from '@app/lineageV3/useComputeGraph/getFineGrainedLineage';

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

    describe('inputFields fallback for Chart endpoints', () => {
        const createChartNode = (
            urn: string,
            inputFieldPaths?: string[],
            schemaFields?: { fieldPath: string }[],
        ): LineageEntity => ({
            id: urn,
            urn,
            type: EntityType.Chart,
            entity: {
                urn,
                type: EntityType.Chart,
                name: 'test chart',
                ...(schemaFields ? { schemaMetadata: { fields: schemaFields } } : {}),
                ...(inputFieldPaths
                    ? {
                          inputFields: {
                              fields: inputFieldPaths.map((fp) => ({
                                  schemaFieldUrn: `urn:li:schemaField:(urn:li:dataset:upstream,${fp})`,
                                  schemaField: { fieldPath: fp },
                              })),
                          },
                      }
                    : {}),
            } as any,
            isExpanded: {} as any,
            fetchStatus: {} as any,
            filters: {} as any,
        });

        it('returns true for Chart with inputFields when schemaMetadata is absent', () => {
            const nodes = new Map([['urn:li:chart:1', createChartNode('urn:li:chart:1', ['revenue', 'cost'])]]);

            expect(schemaFieldExists('urn:li:chart:1', 'revenue', nodes)).toBe(true);
            expect(schemaFieldExists('urn:li:chart:1', 'cost', nodes)).toBe(true);
        });

        it('returns false for Chart with inputFields when field name is not in inputFields', () => {
            const nodes = new Map([['urn:li:chart:1', createChartNode('urn:li:chart:1', ['revenue', 'cost'])]]);

            expect(schemaFieldExists('urn:li:chart:1', 'profit', nodes)).toBe(false);
        });

        it('returns false for Dataset with missing schemaMetadata (existing behavior preserved)', () => {
            const node: LineageEntity = {
                id: 'urn:li:dataset:1',
                urn: 'urn:li:dataset:1',
                type: EntityType.Dataset,
                entity: {
                    urn: 'urn:li:dataset:1',
                    type: EntityType.Dataset,
                    name: 'test dataset',
                } as any,
                isExpanded: {} as any,
                fetchStatus: {} as any,
                filters: {} as any,
            };
            const nodes = new Map([['urn:li:dataset:1', node]]);

            expect(schemaFieldExists('urn:li:dataset:1', 'some_field', nodes)).toBe(false);
        });

        it('normalizes V2 field paths in inputFields comparison', () => {
            const nodes = new Map([
                ['urn:li:chart:1', createChartNode('urn:li:chart:1', ['[version=2.0].[type=string].revenue'])],
            ]);

            expect(schemaFieldExists('urn:li:chart:1', 'revenue', nodes)).toBe(true);
        });

        it('returns false for Chart with no inputFields and no schemaMetadata', () => {
            const nodes = new Map([['urn:li:chart:1', createChartNode('urn:li:chart:1')]]);

            expect(schemaFieldExists('urn:li:chart:1', 'revenue', nodes)).toBe(false);
        });

        // T1.9 (Path 1) deliberately does NOT relax the gate for Dashboard
        // endpoints. V3 GraphQL does not fetch Dashboard inputFields, so a
        // TS fallback would be ineffective. Tracked separately for end-to-end
        // fix.
        it('returns false for Dashboard with inputFields (intentional gap — GraphQL does not fetch Dashboard inputFields)', () => {
            const urn = 'urn:li:dashboard:1';
            const node: LineageEntity = {
                id: urn,
                urn,
                type: EntityType.Dashboard,
                entity: {
                    urn,
                    type: EntityType.Dashboard,
                    name: 'test dashboard',
                    inputFields: {
                        fields: [
                            {
                                schemaFieldUrn: 'urn:li:schemaField:(urn:li:dataset:1,kpi)',
                                schemaField: { fieldPath: 'kpi' },
                            },
                        ],
                    },
                } as any,
                isExpanded: {} as any,
                fetchStatus: {} as any,
                filters: {} as any,
            };
            const nodes = new Map([[urn, node]]);

            expect(schemaFieldExists(urn, 'kpi', nodes)).toBe(false);
            expect(schemaFieldExists(urn, 'missing', nodes)).toBe(false);
        });
    });
});

// Anti-phantom end-to-end tests: verify getFineGrainedLineage itself honours the gate.
describe('getFineGrainedLineage — anti-phantom invariant', () => {
    const datasetUrn = 'urn:li:dataset:(urn:li:dataPlatform:hive,D,PROD)';
    const chartUrn = 'urn:li:chart:(sigma,C)';

    const makeDatasetNode = (fields: string[]): LineageEntity => ({
        id: datasetUrn,
        urn: datasetUrn,
        type: EntityType.Dataset,
        entity: {
            urn: datasetUrn,
            type: EntityType.Dataset,
            name: 'dataset_D',
            schemaMetadata: { fields: fields.map((fieldPath) => ({ fieldPath })) },
        } as any,
        isExpanded: {} as any,
        fetchStatus: {} as any,
        filters: {} as any,
    });

    const makeChartNode = (inputFieldPaths: string[]): LineageEntity => ({
        id: chartUrn,
        urn: chartUrn,
        type: EntityType.Chart,
        entity: {
            urn: chartUrn,
            type: EntityType.Chart,
            name: 'chart_C',
            inputFields: {
                fields: inputFieldPaths.map((fp) => ({
                    schemaFieldUrn: `urn:li:schemaField:(${datasetUrn},${fp})`,
                    schemaField: { fieldPath: fp },
                })),
            },
        } as any,
        isExpanded: {} as any,
        fetchStatus: {} as any,
        filters: {} as any,
    });

    it('drops column edge when upstream Dataset field is absent from schemaMetadata (phantom guard)', () => {
        // Dataset D has only "real"; Chart C points at "ghost" — edge must be dropped.
        const nodes = new Map([
            [datasetUrn, makeDatasetNode(['real'])],
            [chartUrn, makeChartNode(['ghost'])],
        ]);

        const result = getFineGrainedLineage({ nodes, edges: new Map(), rootType: EntityType.Dataset });

        expect(result.indirect.downstream.size).toBe(0);
        expect(result.indirect.upstream.size).toBe(0);
    });

    it('emits column edge when upstream Dataset field exists in schemaMetadata (T1.9 positive path)', () => {
        // Dataset D has "revenue"; Chart C's inputField also points at "revenue" — edge must appear.
        const nodes = new Map([
            [datasetUrn, makeDatasetNode(['revenue'])],
            [chartUrn, makeChartNode(['revenue'])],
        ]);

        const edges = new Map([[createEdgeId(datasetUrn, chartUrn), { isDisplayed: true } as any]]);

        const result = getFineGrainedLineage({ nodes, edges, rootType: EntityType.Dataset });

        // At least one column-level edge must be present in the downstream map.
        expect(result.indirect.downstream.size).toBeGreaterThan(0);
    });
});
