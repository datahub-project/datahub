import { describe, expect, it } from 'vitest';

import { LineageEntity } from '@app/lineageV3/common';
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
});

describe('getFineGrainedLineage', () => {
    const TABLE = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.addresses,PROD)';
    const SIBLING = 'urn:li:dataset:(urn:li:dataPlatform:dbt,db.addresses,PROD)';
    const DOWNSTREAM = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.order_details,PROD)';
    const FIELD = 'address_id';
    const OTHER_FIELD = 'customer_id';

    interface Overrides {
        siblings?: string[];
        /** Siblings arrive as a search for a combined entity and as properties for a separated one,
         *  depending on whether `hideDbtSourceInLineage` is what merged the pair. */
        siblingShape?: 'search' | 'properties';
        fields?: string[];
        fineGrainedLineages?: {
            upstreams: { urn: string; path: string }[];
            downstreams: { urn: string; path: string }[];
        }[];
    }

    function node(
        urn: string,
        { siblings, siblingShape = 'search', fields = [FIELD], fineGrainedLineages }: Overrides = {},
    ): LineageEntity {
        const siblingEntities = siblings?.map((sibling) => ({ urn: sibling }));
        return {
            id: urn,
            urn,
            type: EntityType.Dataset,
            entity: {
                urn,
                type: EntityType.Dataset,
                name: urn,
                schemaMetadata: { fields: fields.map((fieldPath) => ({ fieldPath })) },
                fineGrainedLineages,
                genericEntityProperties:
                    siblingEntities &&
                    (siblingShape === 'search'
                        ? { siblingsSearch: { searchResults: siblingEntities.map((entity) => ({ entity })) } }
                        : { siblings: { isPrimary: true, siblings: siblingEntities } }),
            } as any,
            isExpanded: {} as any,
            fetchStatus: {} as any,
            filters: {} as any,
        };
    }

    function run(nodes: Map<string, LineageEntity>) {
        return getFineGrainedLineage({ nodes, edges: new Map(), rootType: EntityType.Dataset }).indirect;
    }

    function columnEdge(fromUrn: string, fromField: string, toUrn: string, toField: string) {
        return {
            upstreams: [{ urn: fromUrn, path: fromField }],
            downstreams: [{ urn: toUrn, path: toField }],
        };
    }

    // Only dbt emits this edge, and the two siblings are drawn as a single node whether or not
    // `hideDbtSourceInLineage` is what merged them
    it.each(['search', 'properties'] as const)(
        'drops the edge between a column and the same column on a sibling, given as a sibling %s',
        (siblingShape) => {
            const fgl = run(
                new Map([
                    [TABLE, node(TABLE, { siblings: [SIBLING], siblingShape })],
                    [SIBLING, node(SIBLING, { fineGrainedLineages: [columnEdge(TABLE, FIELD, SIBLING, FIELD)] })],
                ]),
            );

            expect(fgl.downstream.size).toEqual(0);
            expect(fgl.upstream.size).toEqual(0);
        },
    );

    it('keeps an edge between different columns on siblings, which is real lineage', () => {
        const fields = [FIELD, OTHER_FIELD];
        const fgl = run(
            new Map([
                [TABLE, node(TABLE, { siblings: [SIBLING], fields })],
                [
                    SIBLING,
                    node(SIBLING, { fields, fineGrainedLineages: [columnEdge(TABLE, FIELD, SIBLING, OTHER_FIELD)] }),
                ],
            ]),
        );

        expect(Array.from(fgl.downstream.get(`${TABLE}::${FIELD}`)?.keys() ?? [])).toEqual([
            `${SIBLING}::${OTHER_FIELD}`,
        ]);
    });

    it('keeps an edge to a column on a node of its own', () => {
        const fgl = run(
            new Map([
                [TABLE, node(TABLE, { siblings: [SIBLING] })],
                [DOWNSTREAM, node(DOWNSTREAM, { fineGrainedLineages: [columnEdge(TABLE, FIELD, DOWNSTREAM, FIELD)] })],
            ]),
        );

        expect(Array.from(fgl.downstream.get(`${TABLE}::${FIELD}`)?.keys() ?? [])).toEqual([`${DOWNSTREAM}::${FIELD}`]);
    });
});
