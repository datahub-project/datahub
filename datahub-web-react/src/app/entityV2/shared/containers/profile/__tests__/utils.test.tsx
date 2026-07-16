import { beforeEach, describe, expect, it, vi } from 'vitest';

import { getEntityPath } from '@app/entityV2/shared/containers/profile/utils';

import { EntityType } from '@types';

// Mock the entity registry
const mockEntityRegistry = {
    getEntityUrl: vi.fn().mockImplementation((entityType, urn, params) => {
        const pathName = entityType.toLowerCase();
        const queryString = params
            ? `?${Object.entries(params)
                  .sort(([keyA], [keyB]) => keyA.localeCompare(keyB))
                  .map(([key, value]) => `${key}=${value}`)
                  .join('&')}`
            : '';
        return `/${pathName}/${urn}${queryString}`;
    }),
};

describe('getEntityPath', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return schema field parent link for SchemaField entity type', () => {
        const result = getEntityPath(
            EntityType.SchemaField,
            'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD),test)',
            mockEntityRegistry as any,
            false,
            false,
        );

        expect(result).toBe(
            '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)/Columns?highlightedPath=test',
        );
    });

    it('should construct URL with lineage mode parameter when no tab name is provided', () => {
        const result = getEntityPath(
            EntityType.Dataset,
            'urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)',
            mockEntityRegistry as any,
            true,
            false,
        );

        expect(result).toBe(
            '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)?is_lineage_mode=true',
        );
    });

    it('should construct URL with tab name and lineage mode parameter', () => {
        const result = getEntityPath(
            EntityType.Dataset,
            'urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)',
            mockEntityRegistry as any,
            true,
            false,
            'properties',
        );

        expect(result).toBe(
            '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)/properties?is_lineage_mode=true',
        );
    });

    it('should include hide sibling mode parameter when enabled', () => {
        const result = getEntityPath(
            EntityType.Dataset,
            'urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)',
            mockEntityRegistry as any,
            true,
            true,
            'properties',
        );

        expect(result).toBe(
            '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)/properties?is_lineage_mode=true&separate_siblings=true',
        );
    });

    it('should include additional tab parameters when provided', () => {
        const tabParams = {
            view: 'list',
            sort: 'name',
        };

        const result = getEntityPath(
            EntityType.Dataset,
            'urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)',
            mockEntityRegistry as any,
            true,
            false,
            'properties',
            tabParams,
        );

        expect(result).toBe(
            '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)/properties?is_lineage_mode=true&sort=name&view=list',
        );
    });

    it('should handle all parameters together', () => {
        const tabParams = {
            view: 'list',
            sort: 'name',
        };

        const result = getEntityPath(
            EntityType.Dataset,
            'urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)',
            mockEntityRegistry as any,
            true,
            true,
            'properties',
            tabParams,
        );

        expect(result).toBe(
            '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)/properties?is_lineage_mode=true&separate_siblings=true&sort=name&view=list',
        );
    });
});
