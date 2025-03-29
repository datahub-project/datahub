import { format } from 'date-fns';
import { SortingState } from '@src/alchemy-components/components/Table/types';
import { EntityType } from '@src/types.generated';
import {
    getFilteredTransformedIncidentData,
    getLinkedAssetsCount,
    getAssigneeWithURN,
    getAssigneeNamesWithAvatarUrl,
    getLinkedAssetsData,
    getFormattedDateForResolver,
    validateForm,
    getSortedIncidents,
    getExistingIncidents,
    useSiblingOptionsForIncidentBuilder,
} from '../utils';
import { IncidentListFilter } from '../types';

describe('Utility Functions', () => {
    test('getFilteredTransformedIncidentData should filter and transform incident data', () => {
        const incidents: any = [
            {
                title: 'Incident 1',
                status: { stage: 'WorkInProgress' },
                incidentType: 'DATASET_COLUMN',
                priority: 'High',
                type: 'INCIDENT',
            },
            {
                title: 'Incident 2',
                status: { stage: 'Closed' },
                incidentType: 'DATASET_COLUMN_1',
                priority: 'Low',
                type: 'INCIDENT',
            },
        ];

        const filter: IncidentListFilter = {
            sortBy: '',
            groupBy: '',
            filterCriteria: {
                searchText: '',
                priority: ['Low'],
                stage: [],
                state: [],
                type: [],
            },
        };

        const result = getFilteredTransformedIncidentData(incidents, filter);

        // Assert total count (total number of incidents)
        expect(result.totalCount).toBe(2);

        // Assert filtered incidents (only matching "Low" priority incidents)
        expect(result.incidents).toHaveLength(1);
        expect(result.incidents).toEqual([
            {
                urn: undefined,
                created: undefined,
                creator: undefined,
                customType: undefined,
                description: undefined,
                stage: 'Closed',
                state: undefined,
                type: 'DATASET_COLUMN_1',
                title: 'Incident 2',
                priority: 'Low',
                linkedAssets: undefined,
                assignees: undefined,
                source: undefined,
                lastUpdated: undefined,
                message: undefined,
            },
        ]);
    });

    test('getLinkedAssetsCount should return formatted asset count', () => {
        expect(getLinkedAssetsCount(999)).toBe('999');
        expect(getLinkedAssetsCount(1000)).toBe('1k');
        expect(getLinkedAssetsCount(1000000)).toBe('1m');
    });

    test('getAssigneeWithURN should return URNs of assignees', () => {
        const assignees = [{ urn: 'urn1' }, { urn: 'urn2' }];
        expect(getAssigneeWithURN(assignees)).toEqual(['urn1', 'urn2']);
    });

    test('getAssigneeNamesWithAvatarUrl should return names with default image URLs', () => {
        const assignees = [{ properties: { displayName: 'John Doe' } }];
        expect(getAssigneeNamesWithAvatarUrl(assignees)).toEqual([{ name: 'John Doe', imageUrl: '' }]);
    });

    test('getLinkedAssetsData should return linked asset URNs', () => {
        const assets = ['urn1', { urn: 'urn2' }];
        expect(getLinkedAssetsData(assets)).toEqual(['urn1', 'urn2']);
    });

    test('getFormattedDateForResolver should return formatted date', () => {
        const timestamp = new Date('2023-10-10T12:00:00Z').getTime();
        expect(getFormattedDateForResolver(timestamp)).toBe(format(new Date(timestamp), "M/d/yyyy 'at' h:mm a"));
    });

    test('validateForm should return true if no errors exist', () => {
        const form = { getFieldsError: () => [{ errors: [] }] };
        expect(validateForm(form)).toBe(true);
    });

    test('getSortedIncidents should sort incidents based on column', () => {
        const incidents = [{ title: 'B' }, { title: 'A' }];
        const record = { incidents };
        const sorted = getSortedIncidents(record, { sortColumn: 'name', sortOrder: SortingState.ASCENDING });
        expect(sorted[0].title).toBe('A');
    });

    test('getExistingIncidents should return combined incidents from entity data', () => {
        const currData = {
            entity: {
                incidents: { incidents: [{ id: 1 }] },
                siblingsSearch: { searchResults: [{ entity: { incidents: { incidents: [{ id: 2 }] } } }] },
            },
        };
        expect(getExistingIncidents(currData)).toEqual([{ id: 1 }, { id: 2 }]);
    });

    test('should return main entity data in options', () => {
        const mockEntityData = {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:bigquery,my_table,PROD)',
            platform: {
                properties: {
                    displayName: 'BigQuery',
                },
                name: 'bigquery',
                urn: 'urn:li:dataPlatform:bigquery',
            },
            dataPlatformInstance: {
                platform: {
                    name: 'BigQueryInstance',
                },
            },
            siblingsSearch: {
                searchResults: [
                    {
                        entity: {
                            urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)',
                            platform: {
                                properties: {
                                    displayName: 'Snowflake',
                                },
                                name: 'snowflake',
                                urn: 'urn:li:dataPlatform:snowflake',
                            },
                            dataPlatformInstance: {
                                platform: {
                                    name: 'SnowflakeInstance',
                                },
                            },
                        },
                    },
                ],
            },
        } as any;
        const result = useSiblingOptionsForIncidentBuilder(
            mockEntityData,
            'urn:li:dataset:main',
            'DATASET' as EntityType,
        );
        expect(result[0]).toEqual({
            title: 'BigQuery',
            urn: 'urn:li:dataset:main',
            platform: mockEntityData.platform,
            entityType: 'DATASET',
        });
    });

    test('should include siblings data in options', () => {
        const mockEntityData = {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:bigquery,my_table,PROD)',
            platform: {
                properties: { displayName: 'BigQuery' },
                name: 'bigquery',
                urn: 'urn:li:dataPlatform:bigquery',
            },
            dataPlatformInstance: {
                platform: { name: 'BigQueryInstance' },
            },
            siblingsSearch: {
                searchResults: [
                    {
                        entity: {
                            urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table1,PROD)',
                            platform: {
                                properties: { displayName: 'Snowflake' },
                                name: 'snowflake',
                                urn: 'urn:li:dataPlatform:snowflake',
                            },
                            dataPlatformInstance: { platform: { name: 'SnowflakeInstance' } },
                            type: 'DATASET',
                        },
                    },
                    {
                        entity: {
                            urn: 'urn:li:dataset:(urn:li:dataPlatform:redshift,my_table2,PROD)',
                            platform: {
                                properties: { displayName: 'Redshift' },
                                name: 'redshift',
                                urn: 'urn:li:dataPlatform:redshift',
                            },
                            dataPlatformInstance: { platform: { name: 'RedshiftInstance' } },
                            type: 'DATASET',
                        },
                    },
                ],
            },
        } as any;

        const result = useSiblingOptionsForIncidentBuilder(mockEntityData, mockEntityData.urn, 'DATASET' as EntityType);

        // Expect 1 main entity + 2 siblings
        expect(result.length).toBe(3);

        expect(result[0]).toEqual({
            urn: mockEntityData.urn,
            title: 'BigQuery',
            platform: mockEntityData.platform,
            entityType: 'DATASET',
        });

        expect(result[1]).toEqual({
            urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table1,PROD)',
            title: 'Snowflake',
            platform: mockEntityData.siblingsSearch.searchResults[0].entity.platform,
            entityType: 'DATASET',
        });

        expect(result[2]).toEqual({
            urn: 'urn:li:dataset:(urn:li:dataPlatform:redshift,my_table2,PROD)',
            title: 'Redshift',
            platform: mockEntityData.siblingsSearch.searchResults[1].entity.platform,
            entityType: 'DATASET',
        });
    });
});
