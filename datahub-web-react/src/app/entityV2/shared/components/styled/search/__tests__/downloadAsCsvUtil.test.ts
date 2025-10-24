import { GenericEntityProperties } from '@app/entity/shared/types';
import {
    getSearchCsvDownloadHeader,
    transformGenericEntityPropertiesToCsvRow,
    transformResultsToCsvRow,
} from '@app/entityV2/shared/components/styled/search/downloadAsCsvUtil';
import { SearchResultInterface } from '@app/entityV2/shared/components/styled/search/types';

import { EntityType } from '@types';

// Mock external dependencies
vi.mock('@app/shared/textUtil', () => ({
    capitalizeFirstLetterOnly: vi.fn((str: string) => str?.charAt(0).toUpperCase() + str?.slice(1).toLowerCase()),
}));

vi.mock('@utils/runtimeBasePath', () => ({
    resolveRuntimePath: vi.fn((path: string) => `/resolved${path}`),
}));

// Mock window.location
Object.defineProperty(window, 'location', {
    value: {
        origin: 'https://example.com',
    },
    writable: true,
});

describe('downloadAsCsvUtil', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('getSearchCsvDownloadHeader', () => {
        it('should return basic header when no sample result provided', () => {
            const result = getSearchCsvDownloadHeader();

            expect(result).toEqual([
                'urn',
                'name',
                'type',
                'description',
                'user owners',
                'user owner emails',
                'group owners',
                'group owner emails',
                'tags',
                'terms',
                'domain',
                'platform',
                'container',
                'entity url',
            ]);
        });

        it('should return basic header when sample result has no degree', () => {
            const sampleResult: SearchResultInterface = {
                entity: { type: EntityType.Dataset, urn: 'test:urn' },
                matchedFields: [],
            };

            const result = getSearchCsvDownloadHeader(sampleResult);

            expect(result).toEqual([
                'urn',
                'name',
                'type',
                'description',
                'user owners',
                'user owner emails',
                'group owners',
                'group owner emails',
                'tags',
                'terms',
                'domain',
                'platform',
                'container',
                'entity url',
            ]);
        });

        it('should include level of dependency when sample result has degree', () => {
            const sampleResult: SearchResultInterface = {
                entity: { type: EntityType.Dataset, urn: 'test:urn' },
                matchedFields: [],
                degree: 2,
            };

            const result = getSearchCsvDownloadHeader(sampleResult);

            expect(result).toEqual([
                'urn',
                'name',
                'type',
                'description',
                'user owners',
                'user owner emails',
                'group owners',
                'group owner emails',
                'tags',
                'terms',
                'domain',
                'platform',
                'container',
                'entity url',
                'level of dependency',
            ]);
        });

        it('should not include level of dependency when degree is not a number', () => {
            const sampleResult: SearchResultInterface = {
                entity: { type: EntityType.Dataset, urn: 'test:urn' },
                matchedFields: [],
                degree: null,
            };

            const result = getSearchCsvDownloadHeader(sampleResult);

            expect(result).not.toContain('level of dependency');
        });
    });

    describe('transformGenericEntityPropertiesToCsvRow', () => {
        let mockEntityRegistry: any;
        let mockResult: any;
        let mockProperties: GenericEntityProperties;

        beforeEach(() => {
            mockEntityRegistry = {
                getDisplayName: vi.fn().mockReturnValue('Test Entity Display Name'),
            };

            mockResult = {
                entity: {
                    type: EntityType.Dataset,
                    urn: 'urn:li:dataset:test',
                },
                matchedFields: [],
            };

            mockProperties = {
                urn: 'urn:li:dataset:test',
                name: 'Test Dataset',
                properties: {
                    description: 'Test description',
                },
                ownership: {
                    owners: [
                        {
                            owner: {
                                type: EntityType.CorpUser,
                                urn: 'urn:li:corpuser:user1',
                                properties: {
                                    fullName: 'John Doe',
                                    displayName: 'John D',
                                    email: 'john@example.com',
                                },
                                editableProperties: {
                                    displayName: 'John Doe Editable',
                                    email: 'john.editable@example.com',
                                },
                            },
                        },
                        {
                            owner: {
                                type: EntityType.CorpGroup,
                                urn: 'urn:li:corpGroup:group1',
                                name: 'Engineering Team',
                                properties: {
                                    email: 'engineering@example.com',
                                },
                            },
                        },
                    ],
                },
                globalTags: {
                    tags: [{ tag: { name: 'tag1' } }, { tag: { name: 'tag2' } }],
                },
                glossaryTerms: {
                    terms: [{ term: { name: 'term1' } }, { term: { name: 'term2' } }],
                },
                domain: {
                    domain: {
                        properties: {
                            name: 'Test Domain',
                        },
                    },
                },
                platform: {
                    name: 'hive',
                    properties: {
                        displayName: 'Apache Hive',
                    },
                },
                container: {
                    properties: {
                        name: 'test-container',
                    },
                },
            } as any;
        });

        it('should transform properties to CSV row correctly', () => {
            const entityUrl = '/entity/dataset/test';
            const result = transformGenericEntityPropertiesToCsvRow(
                mockEntityRegistry,
                mockProperties,
                entityUrl,
                mockResult,
            );

            expect(result).toEqual([
                'urn:li:dataset:test', // urn
                'Test Entity Display Name', // name (from registry)
                'DATASET', // type
                'Test description', // description
                'John Doe Editable', // user owners (editable takes precedence)
                'john.editable@example.com', // user owner emails (editable takes precedence)
                'Engineering Team', // group owners
                'engineering@example.com', // group owner emails
                'tag1,tag2', // tags
                'term1,term2', // terms
                'Test Domain', // domain
                'Apache Hive', // platform (displayName takes precedence)
                'test-container', // container
                'https://example.com/resolved/entity/dataset/test', // entity url
            ]);

            expect(mockEntityRegistry.getDisplayName).toHaveBeenCalledWith(EntityType.Dataset, mockResult.entity);
        });

        it('should handle null properties gracefully', () => {
            const entityUrl = '/entity/dataset/test';
            const result = transformGenericEntityPropertiesToCsvRow(mockEntityRegistry, null, entityUrl, mockResult);

            expect(result).toEqual([
                '', // urn
                'Test Entity Display Name', // name (from registry)
                'DATASET', // type
                '', // description
                '', // user owners
                '', // user owner emails
                '', // group owners
                '', // group owner emails
                '', // tags
                '', // terms
                '', // domain
                '', // platform
                '', // container
                'https://example.com/resolved/entity/dataset/test', // entity url
            ]);
        });

        it('should handle empty properties gracefully', () => {
            const emptyProperties: GenericEntityProperties = {};
            const entityUrl = '/entity/dataset/test';

            const result = transformGenericEntityPropertiesToCsvRow(
                mockEntityRegistry,
                emptyProperties,
                entityUrl,
                mockResult,
            );

            expect(result).toEqual([
                '', // urn
                'Test Entity Display Name', // name (from registry)
                'DATASET', // type
                '', // description
                '', // user owners
                '', // user owner emails
                '', // group owners
                '', // group owner emails
                '', // tags
                '', // terms
                '', // domain
                '', // platform
                '', // container
                'https://example.com/resolved/entity/dataset/test', // entity url
            ]);
        });

        it('should prioritize regular properties over editable properties for description', () => {
            const propertiesWithEditableDescription = {
                ...mockProperties,
                properties: {
                    description: 'Regular description',
                },
                editableProperties: {
                    description: 'Editable description',
                },
            };

            const entityUrl = '/entity/dataset/test';
            const result = transformGenericEntityPropertiesToCsvRow(
                mockEntityRegistry,
                propertiesWithEditableDescription,
                entityUrl,
                mockResult,
            );

            expect(result[3]).toBe('Regular description'); // description field (properties takes precedence)
        });

        it('should fall back to editable description when regular description is not available', () => {
            const propertiesWithOnlyEditableDescription = {
                ...mockProperties,
                properties: {
                    // No description here
                },
                editableProperties: {
                    description: 'Editable description',
                },
            };

            const entityUrl = '/entity/dataset/test';
            const result = transformGenericEntityPropertiesToCsvRow(
                mockEntityRegistry,
                propertiesWithOnlyEditableDescription,
                entityUrl,
                mockResult,
            );

            expect(result[3]).toBe('Editable description'); // description field (fallback to editable)
        });

        it('should handle user owners with different property priorities', () => {
            const propertiesWithVariousUserProps = {
                ...mockProperties,
                ownership: {
                    owners: [
                        {
                            owner: {
                                type: EntityType.CorpUser,
                                urn: 'urn:li:corpuser:user1',
                                properties: {
                                    fullName: 'John Full Name',
                                    displayName: 'John Display',
                                },
                                // No editable properties
                            },
                        },
                        {
                            owner: {
                                type: EntityType.CorpUser,
                                urn: 'urn:li:corpuser:user2',
                                properties: {
                                    fullName: 'Jane Full Name',
                                },
                                editableProperties: {
                                    displayName: 'Jane Editable Display',
                                },
                            },
                        },
                        {
                            owner: {
                                type: EntityType.CorpUser,
                                urn: 'urn:li:corpuser:user3',
                                properties: {
                                    displayName: 'Bob Display Only',
                                },
                            },
                        },
                    ],
                },
            };

            const entityUrl = '/entity/dataset/test';
            const result = transformGenericEntityPropertiesToCsvRow(
                mockEntityRegistry,
                propertiesWithVariousUserProps as any,
                entityUrl,
                mockResult,
            );

            expect(result[4]).toBe('John Full Name,Jane Editable Display,Bob Display Only'); // user owners
        });

        it('should handle platform name capitalization when no display name', () => {
            const propertiesWithPlatformName = {
                ...mockProperties,
                platform: {
                    name: 'mysql',
                    // No properties.displayName
                },
            };

            const entityUrl = '/entity/dataset/test';
            const result = transformGenericEntityPropertiesToCsvRow(
                mockEntityRegistry,
                propertiesWithPlatformName as any,
                entityUrl,
                mockResult,
            );

            expect(result[11]).toBe('Mysql'); // platform field with capitalization
        });

        it('should include degree when present in result', () => {
            const resultWithDegree = {
                ...mockResult,
                degree: 3,
            };

            const entityUrl = '/entity/dataset/test';
            const result = transformGenericEntityPropertiesToCsvRow(
                mockEntityRegistry,
                mockProperties,
                entityUrl,
                resultWithDegree,
            );

            expect(result).toHaveLength(15); // 14 base fields + 1 degree field
            expect(result[14]).toBe('3'); // degree field
        });

        it('should not include degree when not present in result', () => {
            const entityUrl = '/entity/dataset/test';
            const result = transformGenericEntityPropertiesToCsvRow(
                mockEntityRegistry,
                mockProperties,
                entityUrl,
                mockResult,
            );

            expect(result).toHaveLength(14); // 14 base fields only
        });

        it('should handle mixed owner types correctly', () => {
            const propertiesWithMixedOwners = {
                ...mockProperties,
                ownership: {
                    owners: [
                        {
                            owner: {
                                type: EntityType.CorpUser,
                                urn: 'urn:li:corpuser:user1',
                                properties: {
                                    fullName: 'User One',
                                    email: 'user1@example.com',
                                },
                            },
                        },
                        {
                            owner: {
                                type: EntityType.CorpGroup,
                                urn: 'urn:li:corpGroup:group1',
                                name: 'Group One',
                                properties: {
                                    email: 'group1@example.com',
                                },
                            },
                        },
                        {
                            owner: {
                                type: EntityType.CorpUser,
                                urn: 'urn:li:corpuser:user2',
                                properties: {
                                    fullName: 'User Two',
                                    email: 'user2@example.com',
                                },
                            },
                        },
                        {
                            owner: {
                                type: EntityType.CorpGroup,
                                urn: 'urn:li:corpGroup:group2',
                                name: 'Group Two',
                                properties: {
                                    email: 'group2@example.com',
                                },
                            },
                        },
                    ],
                },
            };

            const entityUrl = '/entity/dataset/test';
            const result = transformGenericEntityPropertiesToCsvRow(
                mockEntityRegistry,
                propertiesWithMixedOwners as any,
                entityUrl,
                mockResult,
            );

            expect(result[4]).toBe('User One,User Two'); // user owners
            expect(result[5]).toBe('user1@example.com,user2@example.com'); // user owner emails
            expect(result[6]).toBe('Group One,Group Two'); // group owners
            expect(result[7]).toBe('group1@example.com,group2@example.com'); // group owner emails
        });
    });

    describe('transformResultsToCsvRow', () => {
        let mockEntityRegistry: any;

        beforeEach(() => {
            mockEntityRegistry = {
                getGenericEntityProperties: vi.fn(),
                getEntityUrl: vi.fn(),
                getDisplayName: vi.fn().mockReturnValue('Test Entity'),
            };
        });

        it('should transform multiple results correctly', () => {
            const mockResults: SearchResultInterface[] = [
                {
                    entity: {
                        type: EntityType.Dataset,
                        urn: 'urn:li:dataset:test1',
                    },
                    matchedFields: [],
                },
                {
                    entity: {
                        type: EntityType.Dashboard,
                        urn: 'urn:li:dashboard:test2',
                    },
                    matchedFields: [],
                },
            ];

            const mockProperties1 = {
                urn: 'urn:li:dataset:test1',
                name: 'Dataset 1',
            };

            const mockProperties2 = {
                urn: 'urn:li:dashboard:test2',
                name: 'Dashboard 1',
            };

            mockEntityRegistry.getGenericEntityProperties
                .mockReturnValueOnce(mockProperties1)
                .mockReturnValueOnce(mockProperties2);

            mockEntityRegistry.getEntityUrl
                .mockReturnValueOnce('/entity/dataset/test1')
                .mockReturnValueOnce('/entity/dashboard/test2');

            const result = transformResultsToCsvRow(mockResults, mockEntityRegistry);

            expect(result).toHaveLength(2);
            expect(mockEntityRegistry.getGenericEntityProperties).toHaveBeenCalledTimes(2);
            expect(mockEntityRegistry.getEntityUrl).toHaveBeenCalledTimes(2);

            expect(mockEntityRegistry.getGenericEntityProperties).toHaveBeenCalledWith(
                EntityType.Dataset,
                mockResults[0].entity,
            );
            expect(mockEntityRegistry.getGenericEntityProperties).toHaveBeenCalledWith(
                EntityType.Dashboard,
                mockResults[1].entity,
            );

            expect(mockEntityRegistry.getEntityUrl).toHaveBeenCalledWith(EntityType.Dataset, 'urn:li:dataset:test1');
            expect(mockEntityRegistry.getEntityUrl).toHaveBeenCalledWith(
                EntityType.Dashboard,
                'urn:li:dashboard:test2',
            );
        });

        it('should handle empty results array', () => {
            const result = transformResultsToCsvRow([], mockEntityRegistry);

            expect(result).toEqual([]);
            expect(mockEntityRegistry.getGenericEntityProperties).not.toHaveBeenCalled();
            expect(mockEntityRegistry.getEntityUrl).not.toHaveBeenCalled();
        });

        it('should handle results with degree field', () => {
            const mockResults: SearchResultInterface[] = [
                {
                    entity: {
                        type: EntityType.Dataset,
                        urn: 'urn:li:dataset:test1',
                    },
                    matchedFields: [],
                    degree: 1,
                },
            ];

            const mockProperties = {
                urn: 'urn:li:dataset:test1',
                name: 'Dataset 1',
            };

            mockEntityRegistry.getGenericEntityProperties.mockReturnValue(mockProperties);
            mockEntityRegistry.getEntityUrl.mockReturnValue('/entity/dataset/test1');

            const result = transformResultsToCsvRow(mockResults, mockEntityRegistry);

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(15); // Should include degree field
            expect(result[0][14]).toBe('1'); // degree value
        });
    });
});
