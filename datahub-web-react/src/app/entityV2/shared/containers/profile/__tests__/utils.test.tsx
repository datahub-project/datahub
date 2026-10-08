import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { getEntityPath, useRoutedTab } from '@app/entityV2/shared/containers/profile/utils';
import { EntityTab } from '@app/entityV2/shared/types';

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

const noopComponent = () => null;

function makeTab(overrides: Partial<EntityTab>): EntityTab {
    return {
        name: 'Tab',
        component: noopComponent,
        ...overrides,
    };
}

function renderUseRoutedTab(path: string, tabs: EntityTab[]) {
    return renderHook(() => useRoutedTab(tabs), {
        wrapper: ({ children }) => <MemoryRouter initialEntries={[path]}>{children}</MemoryRouter>,
    });
}

describe('useRoutedTab', () => {
    // Regression test for issue #19658: tab `name` is i18next-translated, so routing must resolve
    // the active tab by a stable, locale-independent `id` rather than the displayed name.
    it('resolves a tab by its stable id when the URL segment does not match the translated name', () => {
        const documentationTab = makeTab({ id: 'Documentation', name: 'Dokumentation' });
        const schemaTab = makeTab({ name: 'Columns' });

        const { result } = renderUseRoutedTab(
            '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)/Documentation',
            [schemaTab, documentationTab],
        );

        expect(result.current).toBe(documentationTab);
    });

    it('falls back to matching on name for tabs that do not declare an id', () => {
        const schemaTab = makeTab({ name: 'Columns' });
        const propertiesTab = makeTab({ name: 'Properties' });

        const { result } = renderUseRoutedTab(
            '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)/Properties',
            [schemaTab, propertiesTab],
        );

        expect(result.current).toBe(propertiesTab);
    });

    it('prefers an id match over a name match', () => {
        // A tab whose name happens to equal another tab's id must not shadow the id match.
        const nameCollisionTab = makeTab({ name: 'Documentation' });
        const documentationTab = makeTab({ id: 'Documentation', name: 'Dokumentation' });

        const { result } = renderUseRoutedTab(
            '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)/Documentation',
            [nameCollisionTab, documentationTab],
        );

        expect(result.current).toBe(documentationTab);
    });

    it('returns undefined when no tab matches the URL segment', () => {
        const schemaTab = makeTab({ name: 'Columns' });

        const { result } = renderUseRoutedTab(
            '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)/DoesNotExist',
            [schemaTab],
        );

        expect(result.current).toBeUndefined();
    });
});
