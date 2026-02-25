import { vi } from 'vitest';

import { removeFromPropertiesList, updatePropertiesList } from '@app/govern/structuredProperties/cacheUtils';

describe('cacheUtils', () => {
    const mockClient = {
        readQuery: vi.fn(),
        writeQuery: vi.fn(),
    };

    const mockInputs = {
        types: ['STRUCTURED_PROPERTY'],
        query: '*',
        start: 0,
        count: 10,
    };

    const mockSearchAcrossEntities = {
        start: 0,
        count: 1,
        total: 1,
        searchResults: [],
        facets: [],
    };

    const mockProperty = {
        urn: 'urn:li:structuredProperty:my-property',
        type: 'STRUCTURED_PROPERTY',
        definition: {
            displayName: 'My Property',
            qualifiedName: 'my-property',
            description: 'This is my property.',
            cardinality: 'SINGLE',
            immutable: false,
            valueType: 'STRING',
            entityTypes: [],
            typeQualifier: null,
            allowedValues: [],
            created: {
                time: 1622547800000,
                actor: 'urn:li:corpuser:datahub',
            },
            lastModified: {
                time: 1622547800000,
                actor: 'urn:li:corpuser:datahub',
            },
        },
        settings: {
            isHidden: false,
            showInSearchFilters: true,
            showAsAssetBadge: true,
            showInAssetSummary: true,
            hideInAssetSummaryWhenEmpty: false,
            showInColumnsTable: true,
        },
        exists: true,
    };

    afterEach(() => {
        vi.clearAllMocks();
    });

    describe('updatePropertiesList', () => {
        it('should add a new property to the cache', () => {
            mockClient.readQuery.mockReturnValue({
                searchAcrossEntities: {
                    searchResults: [],
                },
            });

            updatePropertiesList(mockClient, mockInputs, mockProperty, mockSearchAcrossEntities);

            expect(mockClient.writeQuery).toHaveBeenCalledWith(
                expect.objectContaining({
                    data: expect.objectContaining({
                        searchAcrossEntities: expect.objectContaining({
                            searchResults: expect.arrayContaining([
                                expect.objectContaining({
                                    entity: expect.objectContaining({
                                        urn: mockProperty.urn,
                                    }),
                                }),
                            ]),
                        }),
                    }),
                }),
            );
        });

        it('should not write to the cache if it is empty', () => {
            mockClient.readQuery.mockReturnValue(null);

            updatePropertiesList(mockClient, mockInputs, mockProperty, mockSearchAcrossEntities);

            expect(mockClient.writeQuery).not.toHaveBeenCalled();
        });
    });

    describe('removeFromPropertiesList', () => {
        it('should remove a property from the cache', () => {
            mockClient.readQuery.mockReturnValue({
                searchAcrossEntities: {
                    searchResults: [
                        {
                            entity: {
                                urn: mockProperty.urn,
                            },
                        },
                    ],
                },
            });

            removeFromPropertiesList(mockClient, mockInputs, mockProperty.urn, mockSearchAcrossEntities);

            expect(mockClient.writeQuery).toHaveBeenCalledWith(
                expect.objectContaining({
                    data: expect.objectContaining({
                        searchAcrossEntities: expect.objectContaining({
                            searchResults: [],
                        }),
                    }),
                }),
            );
        });

        it('should not write to the cache if it is empty', () => {
            mockClient.readQuery.mockReturnValue(null);

            removeFromPropertiesList(mockClient, mockInputs, mockProperty.urn, mockSearchAcrossEntities);

            expect(mockClient.writeQuery).not.toHaveBeenCalled();
        });
    });
});
