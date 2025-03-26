import React from 'react';
import { Incident, IncidentPriority, IncidentStage, IncidentState, IncidentType } from '@src/types.generated';
import Fuse from 'fuse.js';
import { getCapitalizeWord } from '@src/alchemy-components/components/IncidentStagePill/utils';
import { SortingState } from '@src/alchemy-components/components/Table/types';
import { format } from 'date-fns';
import {
    IncidentFilterOptions,
    IncidentGroup,
    IncidentGroupBy,
    IncidentListFilter,
    IncidentRecommendedFilter,
    IncidentTable,
    IncidentTableRow,
} from './types';
import {
    INCIDENT_PRIORITY,
    INCIDENT_PRIORITY_NAME_MAP,
    INCIDENT_STAGE_NAME_MAP,
    INCIDENT_STAGES_TYPES,
    INCIDENT_STATE_NAME_MAP,
    INCIDENT_STATES_TYPES,
    INCIDENT_TYPE_NAME_MAP,
    INCIDENT_TYPES,
    PRIORITY_ORDER,
    STAGE_ORDER,
    STATE_ORDER,
} from './constant';

//  Fuse.js setup for search functionality
const fuse = new Fuse<any>([], {
    keys: ['title'],
    threshold: 0.4,
});

const getIncidentGroupTypeIcon = () => {
    return null;
};

const mapLinkedAssetData = (incident) => {
    return incident?.linkedAssets?.relationships
        ?.filter((item) => item.entity?.properties !== null)
        .map((item) => item.entity);
};

const mapIncidentData = (incidents: Incident[]): IncidentTableRow[] => {
    return incidents?.map(
        (incident: Incident) =>
            ({
                urn: incident?.urn,
                created: incident?.created?.time,
                creator: incident?.created,
                customType: incident?.customType,
                description: incident?.description,
                stage: incident?.status?.stage,
                state: incident?.status?.state,
                type: incident?.incidentType,
                title: incident?.title,
                priority: incident?.priority,
                linkedAssets: mapLinkedAssetData(incident),
                assignees: incident?.assignees,
                source: incident?.source,
                lastUpdated: incident?.status?.lastUpdated,
                message: incident?.status?.message,
            } as IncidentTableRow),
    );
};

// Helper function to create incident groups from grouped data
const createIncidentGroupsFromMap = (
    groupedMap: Map<string, Incident[]>,
    keyName: keyof IncidentGroup,
): IncidentGroup[] => {
    const incidentGroups: IncidentGroup[] = [];

    groupedMap.forEach((groupedIncidents, key) => {
        const newGroup: IncidentGroup = {
            name: key,
            icon: getIncidentGroupTypeIcon(),
            untransformedIncidents: groupedIncidents,
            incidents: mapIncidentData(groupedIncidents),
            [keyName]: key, // Dynamically set the group key (type, stage, or priority)
            groupName: (
                <>
                    {getCapitalizeWord(key)} <span>({groupedIncidents.length})</span>
                </>
            ),
        };
        incidentGroups.push(newGroup);
    });

    return incidentGroups;
};

// Helper function to group incidents based on a key
const groupIncidentsBy = (incidents: Incident[], keyExtractor: (incident: Incident) => string | undefined | null) => {
    const groupedMap = new Map<string, Incident[]>();

    incidents.forEach((incident) => {
        const key = keyExtractor(incident) || 'None';
        const groupedIncidents = groupedMap.get(key) || [];
        groupedIncidents.push(incident);
        groupedMap.set(key, groupedIncidents);
    });

    return groupedMap;
};

const orderedIncidents = (priorityIncidentGroups) => {
    const newOrderedIncidents: any = [];
    PRIORITY_ORDER.forEach((priority) => {
        const founItem = priorityIncidentGroups?.find((group) => group?.name === priority);
        if (founItem) {
            newOrderedIncidents.push(founItem);
        }
    });
    return newOrderedIncidents;
};

export const createIncidentGroups = (incidents: Array<Incident>): IncidentGroupBy => {
    // Pre-sort the list of incidents based on which has been most recently created.
    incidents?.sort((a, b) => a?.created?.time - b?.created?.time);

    // Group incidents by type, stage, and priority
    const typeToIncidents = groupIncidentsBy(incidents, (incident) => incident?.incidentType);
    const stageToIncidents = groupIncidentsBy(incidents, (incident) => incident?.status?.stage);
    const stateToIncidents = groupIncidentsBy(incidents, (incident) => incident?.status?.state);
    const priorityToIncidents = groupIncidentsBy(incidents, (incident) => incident.priority);

    // Create IncidentGroup objects for each group
    const typeIncidentGroups = createIncidentGroupsFromMap(typeToIncidents, 'type');
    const stageIncidentGroups = createIncidentGroupsFromMap(stageToIncidents, 'stage');
    const stateIncidentGroups = createIncidentGroupsFromMap(stateToIncidents, 'state');
    const priorityIncidentGroups = createIncidentGroupsFromMap(priorityToIncidents, 'priority');
    return {
        priority: orderedIncidents(priorityIncidentGroups),
        stage: stageIncidentGroups,
        type: typeIncidentGroups,
        state: stateIncidentGroups,
    };
};

const generateStageFilters = (data: IncidentStage[], order: string[]) => {
    const newOrderedArray: any = [];

    if (Array.isArray(data)) {
        // ✅ Type guard ensures 'find' exists
        order.forEach((stage) => {
            const foundItem = data.find((item) => {
                const filter = item as unknown as IncidentRecommendedFilter;
                return filter?.displayName?.toLowerCase() === stage?.toLowerCase();
            });
            if (foundItem) {
                newOrderedArray.push(foundItem);
            }
        });
    }

    return newOrderedArray;
};

const generatePriorityFilters = (data: IncidentPriority[], order: string[]) => {
    const newOrderedArray: any = [];

    if (Array.isArray(data)) {
        // ✅ Type guard ensures 'find' exists
        order.forEach((priority) => {
            const foundItem = data.find((item) => {
                const filter = item as unknown as IncidentRecommendedFilter;
                return filter?.displayName?.toLowerCase() === priority?.toLowerCase();
            });
            if (foundItem) {
                newOrderedArray.push(foundItem);
            }
        });
    }

    return newOrderedArray;
};

const generateStateFilters = (data: IncidentState[], order: string[]) => {
    const newOrderedArray: any = [];

    if (Array.isArray(data)) {
        // ✅ Type guard ensures 'find' exists
        order.forEach((state) => {
            const foundItem = data.find((item) => {
                const filter = item as unknown as IncidentRecommendedFilter;
                return filter?.displayName?.toLowerCase() === state?.toLowerCase();
            });
            if (foundItem) {
                newOrderedArray.push(foundItem);
            }
        });
    }

    return newOrderedArray;
};

// Build the Filter Options as per the type & status
const buildFilterOptions = (key: string, value: Record<string, number>, filterOptions: IncidentFilterOptions) => {
    Object.entries(value).forEach(([name, count]) => {
        let displayName;
        switch (key) {
            case 'type':
                displayName = INCIDENT_TYPE_NAME_MAP[name];
                break;
            case 'stage':
                displayName = INCIDENT_STAGE_NAME_MAP[name];
                break;
            case 'priority':
                displayName = INCIDENT_PRIORITY_NAME_MAP[name];
                break;
            case 'state':
                displayName = INCIDENT_STATE_NAME_MAP[name];
                break;
            default:
                break;
        }

        const filterItem = { name, category: key, count, displayName } as IncidentRecommendedFilter;
        filterOptions.recommendedFilters.push(filterItem);
        filterOptions.filterGroupOptions[key].push(filterItem);
    });

    // Reorder the stage options if the key is 'stage'
    if (key === 'stage') {
        /* eslint-disable-next-line no-param-reassign */
        filterOptions.filterGroupOptions[key] = generateStageFilters(
            filterOptions.filterGroupOptions[key],
            STAGE_ORDER,
        );
    }

    // Reorder the priority options if the key is 'priority'
    if (key === 'priority') {
        /* eslint-disable-next-line no-param-reassign */
        filterOptions.filterGroupOptions[key] = generatePriorityFilters(
            filterOptions.filterGroupOptions[key],
            PRIORITY_ORDER,
        );
    }

    // Reorder the priority options if the key is 'priority'
    if (key === 'state') {
        /* eslint-disable-next-line no-param-reassign */
        filterOptions.filterGroupOptions[key] = generateStateFilters(
            filterOptions.filterGroupOptions[key],
            STATE_ORDER,
        );
    }
};

/** Create filter option list as per the incident data present 
 * for example
 * status :[
 * 
  {
    name: "SUCCESS",
    category: 'status',
    count:10,
    displayName: "Passing"
  }
 * ]
 * 
 * 
*/
const extractFilterOptionListFromIncidents = (incidents: Incident[]) => {
    const filterOptions: IncidentFilterOptions = {
        filterGroupOptions: {
            type: [],
            stage: [],
            priority: [],
            state: [],
        },
        recommendedFilters: [],
    };

    const filterGroupCounts = {
        type: {} as Record<string, number>,
        stage: {} as Record<string, number>,
        state: {} as Record<string, number>,
        priority: {} as Record<string, number>,
    };

    // maintain array to show all the Incident Type count even if it is not present
    const remainingIncidentTypes: IncidentType[] = [...INCIDENT_TYPES];
    const remainingIncidentStages = [...INCIDENT_STAGES_TYPES];
    const remainingIncidentStates = [...INCIDENT_STATES_TYPES];
    const remainingIncidentPriorities = [...INCIDENT_PRIORITY];

    incidents.forEach((incident: Incident) => {
        // filter out tracked types
        const type = incident.incidentType as IncidentType;
        if (type) {
            const index = remainingIncidentTypes.indexOf(type);
            if (index > -1) {
                remainingIncidentTypes.splice(index, 1);
            }
            filterGroupCounts.type[type] = (filterGroupCounts.type[type] || 0) + 1;
        }

        // filter out tracked stages
        const stage = incident.status.stage as IncidentStage;
        if (stage) {
            const stageIndex = remainingIncidentStages.indexOf(stage);
            if (stageIndex > -1) {
                remainingIncidentStages.splice(stageIndex, 1);
            }

            filterGroupCounts.stage[stage] = (filterGroupCounts.stage[stage] || 0) + 1;
        }

        // filter out tracked states
        const state = incident.status.state as IncidentState;
        if (state) {
            const stateIndex = remainingIncidentStates.indexOf(state);
            if (stateIndex > -1) {
                remainingIncidentStates.splice(stateIndex, 1);
            }

            filterGroupCounts.state[state] = (filterGroupCounts.state[state] || 0) + 1;
        }

        // filter out tracked priorities
        const priority = (incident.priority || 'None') as IncidentPriority;
        const priorityIndex = remainingIncidentPriorities.indexOf(priority);
        if (priorityIndex > -1) {
            remainingIncidentPriorities.splice(priorityIndex, 1);
        }

        filterGroupCounts.priority[priority] = (filterGroupCounts.priority[priority] || 0) + 1;
    });

    // Add remaining Incident type with count 0
    remainingIncidentTypes.forEach((incidentType: IncidentType) => {
        filterGroupCounts.type[incidentType] = 0;
    });

    // Add remaining Incident status with count 0
    remainingIncidentStages.forEach((stage: IncidentStage) => {
        filterGroupCounts.stage[stage] = 0;
    });

    // Add remaining Incident status with count 0
    remainingIncidentStates.forEach((state: IncidentState) => {
        filterGroupCounts.state[state] = 0;
    });

    // Add remaining Incident status with count 0
    remainingIncidentPriorities.forEach((incidentPriority: IncidentPriority) => {
        filterGroupCounts.priority[incidentPriority] = 0;
    });

    buildFilterOptions('type', filterGroupCounts.type, filterOptions);
    buildFilterOptions('stage', filterGroupCounts.stage, filterOptions);
    buildFilterOptions('priority', filterGroupCounts.priority, filterOptions);
    buildFilterOptions('state', filterGroupCounts.state, filterOptions);
    return filterOptions;
};

// Assign Filtered Incidents to group
const assignFilteredIncidentToGroup = (filteredIncidents: Incident[]): IncidentTable => {
    const incidentRawData: IncidentTable = {
        incidents: [],
        groupBy: { type: [], stage: [], priority: [], state: [] },
        filterOptions: {},
    };
    incidentRawData.incidents = mapIncidentData(filteredIncidents);
    incidentRawData.groupBy = createIncidentGroups(filteredIncidents);
    incidentRawData.filterOptions = extractFilterOptionListFromIncidents(filteredIncidents);
    return incidentRawData;
};

const getFilteredIncidents = (incidents: Incident[], filter: IncidentListFilter) => {
    const { priority, stage, type, state } = filter.filterCriteria;

    // Apply type, priority, and stage
    return incidents.filter((incident: Incident) => {
        const matchesCategory = type.length === 0 || type.includes(incident.incidentType);
        const matchesPriority = priority.length === 0 || priority.includes(incident.priority || 'None');
        const matchesStage = stage.length === 0 || stage.includes(incident.status.stage || 'None');
        const matchesState = state.length === 0 || state.includes(incident.status.state);
        return matchesCategory && matchesPriority && matchesStage && matchesState;
    });
};

/** Return return filter incident as per selected type status and other things
 * it returns transformated into
 * 1. group of incidents as per type , status
 * 2. Transform data into {@link IncidentListTableRow }  data
 * 2. Filter out incidents as per the search text
 * 3. filter out incidents as per the selected type and status
 */
export const getFilteredTransformedIncidentData = (incidents: Incident[], filter: IncidentListFilter): any => {
    // Apply search filter if searchText is provided
    let filteredIncidents = incidents;
    const { searchText } = filter.filterCriteria;
    let searchMatchesCount = 0;

    if (searchText) {
        fuse.setCollection(incidents || []);
        const result = fuse.search(searchText);
        filteredIncidents = result.map((match) => match.item);
        searchMatchesCount = filteredIncidents.length;
    }

    // Apply type, status, and other filters
    filteredIncidents = getFilteredIncidents(filteredIncidents, filter);

    // Transform filtered incidents
    const incidentRawData = assignFilteredIncidentToGroup(filteredIncidents);
    incidentRawData.searchMatchesCount = searchMatchesCount;
    incidentRawData.totalCount = incidents?.length;
    incidentRawData.originalFilterOptions = extractFilterOptionListFromIncidents(incidents);
    return incidentRawData;
};

export const getLinkedAssetsCount = (asset: number): string => {
    const units = ['k', 'm', 'b'];
    let unitIndex = -1;

    while (asset >= 1000 && unitIndex < units.length - 1) {
        /* eslint-disable-next-line no-param-reassign */
        asset /= 1000;
        unitIndex++;
    }

    return unitIndex === -1
        ? asset.toString()
        : Intl.NumberFormat('en', { maximumFractionDigits: 1 }).format(asset) + units[unitIndex];
};

export const getAssigneeWithURN = (assignees) => {
    return assignees?.map((assignee) => assignee.urn);
};

export const getAssigneeNamesWithAvatarUrl = (assignees) => {
    return assignees?.map((assignee) => {
        return {
            urn: assignee.urn,
            name: assignee.properties?.displayName || assignee.username,
            imageUrl: '',
        };
    });
};

export const getLinkedAssetsData = (assets) => {
    return assets?.map((item) => (typeof item === 'string' ? item : item.urn));
};

export const getFormattedDateForResolver = (lastUpdate) => {
    // Create a Date object from the timestamp
    const date = new Date(lastUpdate);

    // Format the date and time
    const resolvedDateTime = format(date, "M/d/yyyy 'at' h:mm a");

    return resolvedDateTime;
};

// Helper function for validating the form errors
export const validateForm = (form) => !form.getFieldsError().some(({ errors }) => errors.length > 0);

export const getSortedIncidents = (record: any, sortedOptions: { sortColumn: string; sortOrder: SortingState }) => {
    const { sortOrder, sortColumn } = sortedOptions;

    if (sortOrder === SortingState.ORIGINAL) return record.incidents;

    const localeOptions = { sensitivity: 'base' };

    const sortFunctions = {
        created: (a, b) => (sortOrder === SortingState.ASCENDING ? b.created - a.created : a.created - b.created),
        name: (a, b) =>
            sortOrder === SortingState.ASCENDING
                ? (a.title || '').localeCompare(b.title || '', undefined, localeOptions)
                : (b.title || '').localeCompare(a.title || '', undefined, localeOptions),
        type: (a, b) =>
            sortOrder === SortingState.ASCENDING
                ? (a.type || '').localeCompare(b.type || '', undefined, localeOptions)
                : (b.type || '').localeCompare(a.type || '', undefined, localeOptions),
        linkedAssets: (a, b) =>
            sortOrder === SortingState.ASCENDING
                ? b.linkedAssets.length - a.linkedAssets.length
                : a.linkedAssets.length - b.linkedAssets.length,
        assignees: (a, b) =>
            sortOrder === SortingState.ASCENDING
                ? b.assignees?.length - a.assignees?.length
                : a.assignees?.length - b.assignees?.length,
    };

    const sortFunction = sortFunctions[sortColumn];
    return sortFunction ? [...record.incidents].sort(sortFunction) : record.incidents;
};

export const getExistingIncidents = (currData) => {
    return [
        ...(currData?.entity?.incidents?.incidents || []),
        ...(currData?.entity?.siblingsSearch?.searchResults[0]?.entity?.incidents?.incidents || []),
    ];
};
