import { describe, expect, it } from 'vitest';

import { FetchStatus, LineageEntity } from '@app/lineageV3/common';
import { FetchedEntityV2, LineageAsset, LineageAssetType } from '@app/lineageV3/types';
import getFineGrainedLineage, { schemaFieldExists } from '@app/lineageV3/useComputeGraph/getFineGrainedLineage';

import { EntityType, LineageDirection } from '@types';

describe('schemaFieldExists', () => {
    it('should return false when node does not exist in nodes map', () => {
        const nodes = new Map<string, LineageEntity>();
        const datasetUrn = 'urn:li:dataset:(test,table1,PROD)';
        const fieldPath = 'field1';

        const result = schemaFieldExists(datasetUrn, fieldPath, nodes);

        expect(result).toBe(false);
    });

    it('should return false when node exists but entity is undefined', () => {
        const nodes = new Map<string, LineageEntity>();
        const datasetUrn = 'urn:li:dataset:(test,table1,PROD)';
        const fieldPath = 'field1';

        const mockNode: LineageEntity = {
            id: 'node1',
            urn: datasetUrn,
            type: EntityType.Dataset,
            isExpanded: { [LineageDirection.Upstream]: false, [LineageDirection.Downstream]: false },
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.LOADING,
                [LineageDirection.Downstream]: FetchStatus.LOADING,
            },
            filters: {
                [LineageDirection.Upstream]: { facetFilters: new Map() },
                [LineageDirection.Downstream]: { facetFilters: new Map() },
            },
            entity: undefined,
        };

        nodes.set(datasetUrn, mockNode);

        const result = schemaFieldExists(datasetUrn, fieldPath, nodes);

        expect(result).toBe(false);
    });

    it('should return false when entity exists but lineageAssets is undefined', () => {
        const nodes = new Map<string, LineageEntity>();
        const datasetUrn = 'urn:li:dataset:(test,table1,PROD)';
        const fieldPath = 'field1';

        const mockEntity: FetchedEntityV2 = {
            urn: datasetUrn,
            type: EntityType.Dataset,
            name: 'Test Dataset',
            lineageAssets: undefined,
        };

        const mockNode: LineageEntity = {
            id: 'node1',
            urn: datasetUrn,
            type: EntityType.Dataset,
            isExpanded: { [LineageDirection.Upstream]: false, [LineageDirection.Downstream]: false },
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.LOADING,
                [LineageDirection.Downstream]: FetchStatus.LOADING,
            },
            filters: {
                [LineageDirection.Upstream]: { facetFilters: new Map() },
                [LineageDirection.Downstream]: { facetFilters: new Map() },
            },
            entity: mockEntity,
        };

        nodes.set(datasetUrn, mockNode);

        const result = schemaFieldExists(datasetUrn, fieldPath, nodes);

        expect(result).toBe(false);
    });

    it('should return true when field exists in lineageAssets', () => {
        const nodes = new Map<string, LineageEntity>();
        const datasetUrn = 'urn:li:dataset:(test,table1,PROD)';
        const fieldPath = 'field1';

        const mockLineageAsset: LineageAsset = {
            type: LineageAssetType.Column,
            name: 'field1',
        };

        const lineageAssets = new Map<string, LineageAsset>();
        lineageAssets.set('field1', mockLineageAsset); // downgradeV2FieldPath('field1') = 'field1'

        const mockEntity: FetchedEntityV2 = {
            urn: datasetUrn,
            type: EntityType.Dataset,
            name: 'Test Dataset',
            lineageAssets,
        };

        const mockNode: LineageEntity = {
            id: 'node1',
            urn: datasetUrn,
            type: EntityType.Dataset,
            isExpanded: { [LineageDirection.Upstream]: false, [LineageDirection.Downstream]: false },
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.LOADING,
                [LineageDirection.Downstream]: FetchStatus.LOADING,
            },
            filters: {
                [LineageDirection.Upstream]: { facetFilters: new Map() },
                [LineageDirection.Downstream]: { facetFilters: new Map() },
            },
            entity: mockEntity,
        };

        nodes.set(datasetUrn, mockNode);

        const result = schemaFieldExists(datasetUrn, fieldPath, nodes);

        expect(result).toBe(true);
    });

    it('should return false when field does not exist in lineageAssets', () => {
        const nodes = new Map<string, LineageEntity>();
        const datasetUrn = 'urn:li:dataset:(test,table1,PROD)';
        const fieldPath = 'field1';

        const mockLineageAsset: LineageAsset = {
            type: LineageAssetType.Column,
            name: 'field2',
        };

        const lineageAssets = new Map<string, LineageAsset>();
        lineageAssets.set('field2', mockLineageAsset); // Different field

        const mockEntity: FetchedEntityV2 = {
            urn: datasetUrn,
            type: EntityType.Dataset,
            name: 'Test Dataset',
            lineageAssets,
        };

        const mockNode: LineageEntity = {
            id: 'node1',
            urn: datasetUrn,
            type: EntityType.Dataset,
            isExpanded: { [LineageDirection.Upstream]: false, [LineageDirection.Downstream]: false },
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.LOADING,
                [LineageDirection.Downstream]: FetchStatus.LOADING,
            },
            filters: {
                [LineageDirection.Upstream]: { facetFilters: new Map() },
                [LineageDirection.Downstream]: { facetFilters: new Map() },
            },
            entity: mockEntity,
        };

        nodes.set(datasetUrn, mockNode);

        const result = schemaFieldExists(datasetUrn, fieldPath, nodes);

        expect(result).toBe(false);
    });

    it('should handle field path with array annotations and find the downgraded field', () => {
        const nodes = new Map<string, LineageEntity>();
        const datasetUrn = 'urn:li:dataset:(test,table1,PROD)';
        const fieldPath = '[version=2.0].[type=properties].map.[type=text].uiState';

        const mockLineageAsset: LineageAsset = {
            type: LineageAssetType.Column,
            name: 'map.uiState',
        };

        const lineageAssets = new Map<string, LineageAsset>();
        // Store the asset with the downgraded field path (array annotations removed)
        lineageAssets.set('map.uiState', mockLineageAsset);

        const mockEntity: FetchedEntityV2 = {
            urn: datasetUrn,
            type: EntityType.Dataset,
            name: 'Test Dataset',
            lineageAssets,
        };

        const mockNode: LineageEntity = {
            id: 'node1',
            urn: datasetUrn,
            type: EntityType.Dataset,
            isExpanded: { [LineageDirection.Upstream]: false, [LineageDirection.Downstream]: false },
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.LOADING,
                [LineageDirection.Downstream]: FetchStatus.LOADING,
            },
            filters: {
                [LineageDirection.Upstream]: { facetFilters: new Map() },
                [LineageDirection.Downstream]: { facetFilters: new Map() },
            },
            entity: mockEntity,
        };

        nodes.set(datasetUrn, mockNode);

        const result = schemaFieldExists(datasetUrn, fieldPath, nodes);

        expect(result).toBe(true);
    });

    it('should handle empty lineageAssets map', () => {
        const nodes = new Map<string, LineageEntity>();
        const datasetUrn = 'urn:li:dataset:(test,table1,PROD)';
        const fieldPath = 'field1';

        const lineageAssets = new Map<string, LineageAsset>(); // Empty map

        const mockEntity: FetchedEntityV2 = {
            urn: datasetUrn,
            type: EntityType.Dataset,
            name: 'Test Dataset',
            lineageAssets,
        };

        const mockNode: LineageEntity = {
            id: 'node1',
            urn: datasetUrn,
            type: EntityType.Dataset,
            isExpanded: { [LineageDirection.Upstream]: false, [LineageDirection.Downstream]: false },
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.LOADING,
                [LineageDirection.Downstream]: FetchStatus.LOADING,
            },
            filters: {
                [LineageDirection.Upstream]: { facetFilters: new Map() },
                [LineageDirection.Downstream]: { facetFilters: new Map() },
            },
            entity: mockEntity,
        };

        nodes.set(datasetUrn, mockNode);

        const result = schemaFieldExists(datasetUrn, fieldPath, nodes);

        expect(result).toBe(false);
    });

    it('should return false when field path downgrades but still does not exist', () => {
        const nodes = new Map<string, LineageEntity>();
        const datasetUrn = 'urn:li:dataset:(test,table1,PROD)';
        const fieldPath = 'missing[0].field'; // Downgrades to 'missing.field' but doesn't exist

        const mockLineageAsset: LineageAsset = {
            type: LineageAssetType.Column,
            name: 'existing.field',
        };

        const lineageAssets = new Map<string, LineageAsset>();
        lineageAssets.set('existing.field', mockLineageAsset); // Different field

        const mockEntity: FetchedEntityV2 = {
            urn: datasetUrn,
            type: EntityType.Dataset,
            name: 'Test Dataset',
            lineageAssets,
        };

        const mockNode: LineageEntity = {
            id: 'node1',
            urn: datasetUrn,
            type: EntityType.Dataset,
            isExpanded: { [LineageDirection.Upstream]: false, [LineageDirection.Downstream]: false },
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.LOADING,
                [LineageDirection.Downstream]: FetchStatus.LOADING,
            },
            filters: {
                [LineageDirection.Upstream]: { facetFilters: new Map() },
                [LineageDirection.Downstream]: { facetFilters: new Map() },
            },
            entity: mockEntity,
        };

        nodes.set(datasetUrn, mockNode);

        const result = schemaFieldExists(datasetUrn, fieldPath, nodes);

        expect(result).toBe(false);
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
        const lineageAssets = new Map<string, LineageAsset>(
            fields.map((fieldPath) => [fieldPath, { type: LineageAssetType.Column, name: fieldPath }]),
        );
        return {
            id: urn,
            urn,
            type: EntityType.Dataset,
            entity: {
                urn,
                type: EntityType.Dataset,
                name: urn,
                schemaMetadata: { fields: fields.map((fieldPath) => ({ fieldPath })) },
                lineageAssets,
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

    // Only dbt emits this edge. It is kept: siblings are drawn as separate nodes (e.g. a dbt model
    // as a transformation node), so the edge is drawable, and it lets column lineage pass through
    // a hidden sibling.
    it.each(['search', 'properties'] as const)(
        'keeps the edge between a column and the same column on a sibling, given as a sibling %s',
        (siblingShape) => {
            const fgl = run(
                new Map([
                    [TABLE, node(TABLE, { siblings: [SIBLING], siblingShape })],
                    [SIBLING, node(SIBLING, { fineGrainedLineages: [columnEdge(TABLE, FIELD, SIBLING, FIELD)] })],
                ]),
            );

            expect(Array.from(fgl.downstream.get(`${TABLE}::${FIELD}`)?.keys() ?? [])).toEqual([
                `${SIBLING}::${FIELD}`,
            ]);
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
