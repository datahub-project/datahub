import { format } from 'date-fns';

import { IncidentListFilter } from '@app/entityV2/shared/tabs/Incident/types';
import {
    getAssigneeNamesWithAvatarUrl,
    getAssigneeWithURN,
    getExistingIncidents,
    getFilteredTransformedIncidentData,
    getFormattedDateForResolver,
    getLinkedAssetsCount,
    getLinkedAssetsData,
    getSortedIncidents,
    useSiblingOptionsForIncidentBuilder,
    validateForm,
} from '@app/entityV2/shared/tabs/Incident/utils';
import { SortingState } from '@src/alchemy-components/components/Table/types';
import { EntityType } from '@src/types.generated';

describe('Utility Functions', () => {
    test('getFilteredTransformedIncidentData should filter and transform incident data', () => {
        const incidents: any = [
            {
                title: 'Incident 1',
                incidentStatus: { stage: 'WorkInProgress' },
                incidentType: 'DATASET_COLUMN',
                priority: 'High',
                type: 'INCIDENT',
            },
            {
                title: 'Incident 2',
                incidentStatus: { stage: 'Closed' },
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
                category: [],
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
        expect(getExistingIncidents(currData)).toEqual([{ id: 1 }]);
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

// Regression tests locking the CURRENT (pre-i18n-refactor) behavior of the filter
// grouping/ordering pipeline. The upcoming refactor changes the generate*Filters
// helpers to match on the raw enum `name` instead of the translated `displayName`,
// and turns the *_ORDER arrays + *_NAME_MAP constants into i18next getters. The
// canonical ordering and the English displayName values produced here MUST stay
// identical after that refactor.
describe('getFilteredTransformedIncidentData - filter option ordering & display names', () => {
    const noFilter: IncidentListFilter = {
        sortBy: '',
        groupBy: '',
        filterCriteria: {
            searchText: '',
            priority: [],
            stage: [],
            state: [],
            category: [],
        },
    };

    // Build incidents whose stage/priority/state appear in a deliberately
    // non-canonical order so the ordering logic is actually exercised.
    const unorderedIncidents: any = [
        {
            title: 'A',
            incidentStatus: { stage: 'FIXED', state: 'RESOLVED' },
            incidentType: 'OPERATIONAL',
            priority: 'LOW',
            type: 'INCIDENT',
            created: { time: 1 },
        },
        {
            title: 'B',
            incidentStatus: { stage: 'TRIAGE', state: 'ACTIVE' },
            incidentType: 'FRESHNESS',
            priority: 'CRITICAL',
            type: 'INCIDENT',
            created: { time: 2 },
        },
        {
            title: 'C',
            incidentStatus: { stage: 'WORK_IN_PROGRESS', state: 'ACTIVE' },
            incidentType: 'VOLUME',
            priority: 'MEDIUM',
            type: 'INCIDENT',
            created: { time: 3 },
        },
        {
            title: 'D',
            incidentStatus: { stage: 'NO_ACTION_REQUIRED', state: 'RESOLVED' },
            incidentType: 'SQL',
            priority: 'HIGH',
            type: 'INCIDENT',
            created: { time: 4 },
        },
        {
            title: 'E',
            incidentStatus: { stage: 'INVESTIGATION', state: 'ACTIVE' },
            incidentType: 'FIELD',
            priority: 'HIGH',
            type: 'INCIDENT',
            created: { time: 5 },
        },
    ];

    test('stage filter options come back in canonical order (Triage -> Investigation -> In progress -> Fixed -> No Action)', () => {
        const result = getFilteredTransformedIncidentData(unorderedIncidents, noFilter);
        const stageOptions = result.filterOptions.filterGroupOptions.stage;

        expect(stageOptions.map((o) => o.name)).toEqual([
            'TRIAGE',
            'INVESTIGATION',
            'WORK_IN_PROGRESS',
            'FIXED',
            'NO_ACTION_REQUIRED',
        ]);
        expect(stageOptions.map((o) => o.displayName)).toEqual([
            'Triage',
            'Investigation',
            'In Progress',
            'Fixed',
            'No Action',
        ]);
    });

    test('priority filter options come back in canonical order (Critical -> High -> Medium -> Low)', () => {
        const result = getFilteredTransformedIncidentData(unorderedIncidents, noFilter);
        const priorityOptions = result.filterOptions.filterGroupOptions.priority;

        expect(priorityOptions.map((o) => o.name)).toEqual(['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']);
        expect(priorityOptions.map((o) => o.displayName)).toEqual(['Critical', 'High', 'Medium', 'Low']);
    });

    test('state filter options come back in canonical order (Active -> Resolved)', () => {
        const result = getFilteredTransformedIncidentData(unorderedIncidents, noFilter);
        const stateOptions = result.filterOptions.filterGroupOptions.state;

        expect(stateOptions.map((o) => o.name)).toEqual(['ACTIVE', 'RESOLVED']);
        expect(stateOptions.map((o) => o.displayName)).toEqual(['Active', 'Resolved']);
    });

    test('each ordered filter option carries category and aggregated count', () => {
        const result = getFilteredTransformedIncidentData(unorderedIncidents, noFilter);
        const priorityOptions = result.filterOptions.filterGroupOptions.priority;

        // 'HIGH' appears twice (incidents D and E), the rest once.
        const high = priorityOptions.find((o) => o.name === 'HIGH');
        expect(high).toMatchObject({ name: 'HIGH', category: 'priority', count: 2, displayName: 'High' });

        const low = priorityOptions.find((o) => o.name === 'LOW');
        expect(low).toMatchObject({ name: 'LOW', category: 'priority', count: 1, displayName: 'Low' });
    });

    test('category display names map raw enum types to English labels', () => {
        const result = getFilteredTransformedIncidentData(unorderedIncidents, noFilter);
        const categoryOptions = result.filterOptions.filterGroupOptions.category;
        const byName = Object.fromEntries(categoryOptions.map((o) => [o.name, o.displayName]));

        expect(byName.OPERATIONAL).toBe('Operational');
        expect(byName.FRESHNESS).toBe('Freshness');
        expect(byName.VOLUME).toBe('Volume');
        expect(byName.SQL).toBe('SQL');
        expect(byName.FIELD).toBe('Column');
    });

    test('groupBy output exposes ordered priority groups and group counts', () => {
        const result = getFilteredTransformedIncidentData(unorderedIncidents, noFilter);

        // priority groups are ordered via PRIORITY_ORDER (Critical, High, Medium, Low).
        expect(result.groupBy.priority.map((g) => g.name)).toEqual(['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']);

        const highGroup = result.groupBy.priority.find((g) => g.name === 'HIGH');
        expect(highGroup.incidents).toHaveLength(2);
        expect(highGroup.priority).toBe('HIGH');
    });

    test('originalFilterOptions reflects the full unfiltered set even when a filter is applied', () => {
        const filteredToCritical: IncidentListFilter = {
            ...noFilter,
            filterCriteria: { ...noFilter.filterCriteria, priority: ['CRITICAL'] },
        };
        const result = getFilteredTransformedIncidentData(unorderedIncidents, filteredToCritical);

        // Only the single CRITICAL incident survives filtering...
        expect(result.incidents).toHaveLength(1);
        expect(result.incidents[0].priority).toBe('CRITICAL');

        // ...but originalFilterOptions still aggregates across all incidents.
        const originalPriority = result.originalFilterOptions.filterGroupOptions.priority;
        expect(originalPriority.find((o) => o.name === 'HIGH')?.count).toBe(2);
    });
});
