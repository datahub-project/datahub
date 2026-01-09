import { renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import useStructuredProperties from '@app/entityV2/summary/properties/hooks/useStructuredProperties';
import * as governUtils from '@app/govern/structuredProperties/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, SummaryElementType } from '@types';

vi.mock('@app/entity/shared/EntityContext');
vi.mock('@app/useEntityRegistry');
vi.mock('@graphql/search.generated');
vi.mock('@app/govern/structuredProperties/utils', async () => {
    const actual = await vi.importActual('@app/govern/structuredProperties/utils');
    return {
        ...actual,
        getEntityTypesPropertyFilter: vi.fn(),
        getNotHiddenPropertyFilter: vi.fn(),
        getValueTypeFilter: vi.fn(),
        getDisplayNameFilter: vi.fn(),
        getStructuredPropertiesSearchInputs: vi.fn(),
    };
});

const mockSearchResults = {
    searchAcrossEntities: {
        searchResults: [
            {
                entity: {
                    __typename: 'StructuredProperty',
                    type: EntityType.StructuredProperty,
                    urn: 'urn1',
                    definition: {
                        displayName: 'First Property',
                        valueType: {
                            urn: 'urn:li:dataType:datahub.string',
                        },
                    },
                },
            },
            {
                entity: {
                    __typename: 'StructuredProperty',
                    type: EntityType.StructuredProperty,
                    urn: 'urn2',
                    definition: {
                        displayName: 'Second Property',
                        valueType: {
                            urn: 'urn:li:dataType:datahub.string',
                        },
                    },
                },
            },
            {
                entity: {
                    __typename: 'StructuredProperty',
                    type: EntityType.StructuredProperty,
                    urn: 'urn3',
                    definition: {
                        displayName: 'Rich Text Property',
                        valueType: {
                            urn: 'urn:li:dataType:datahub.rich_text', // should get filtered out
                        },
                    },
                },
            },
            {
                entity: {
                    __typename: 'Dataset', // Not a structured property
                    type: EntityType.Dataset,
                    urn: 'urn3',
                },
            },
        ],
    },
};

describe('useStructuredProperties', () => {
    beforeEach(() => {
        vi.resetAllMocks();
        (useEntityData as any).mockReturnValue({ entityType: EntityType.Dataset });
        (useEntityRegistry as any).mockReturnValue({});
        (governUtils.getEntityTypesPropertyFilter as any).mockReturnValue({ field: 'test', values: [] });
        (governUtils.getNotHiddenPropertyFilter as any).mockReturnValue({ field: 'test', values: [] });
        (governUtils.getValueTypeFilter as any).mockReturnValue({ field: 'test', values: [] });
        (governUtils.getDisplayNameFilter as any).mockReturnValue({ field: 'test', values: [] });
        (governUtils.getStructuredPropertiesSearchInputs as any).mockReturnValue({ field: 'test', values: [] });
    });

    it('should correctly map search results to AssetProperty', () => {
        (useGetSearchResultsForMultipleQuery as any).mockReturnValue({
            data: mockSearchResults,
            loading: false,
        });
        const { result } = renderHook(() => useStructuredProperties(''));
        expect(result.current.structuredProperties).toHaveLength(2);
        const firstProperty = result.current.structuredProperties[0];
        expect(firstProperty.name).toBe('First Property');
        expect(firstProperty.key).toBe('urn1');
        expect(firstProperty.type).toBe(SummaryElementType.StructuredProperty);
        expect(firstProperty.structuredProperty?.urn).toBe('urn1');
        expect(firstProperty.structuredProperty?.definition.displayName).toBe('First Property');
    });

    it('should not call getDisplayNameFilter when query is empty', () => {
        (useGetSearchResultsForMultipleQuery as any).mockReturnValue({ data: null, loading: false });
        renderHook(() => useStructuredProperties(''));
        expect(governUtils.getDisplayNameFilter).not.toHaveBeenCalled();
    });

    it('should return loading state from useGetSearchResultsForMultipleQuery', () => {
        (useGetSearchResultsForMultipleQuery as any).mockReturnValue({ data: null, loading: true });
        const { result } = renderHook(() => useStructuredProperties(''));
        expect(result.current.loading).toBe(true);
    });
});
