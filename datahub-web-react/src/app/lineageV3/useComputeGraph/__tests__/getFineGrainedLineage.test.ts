import { describe, expect, it } from 'vitest';

import { FetchStatus, LineageEntity } from '@app/lineageV3/common';
import { FetchedEntityV2, LineageAsset, LineageAssetType } from '@app/lineageV3/types';
import { schemaFieldExists } from '@app/lineageV3/useComputeGraph/getFineGrainedLineage';

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
