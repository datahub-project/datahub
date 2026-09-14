import { Text } from '@components';
import Fuse from 'fuse.js';
import i18next from 'i18next';
import React from 'react';
import styled from 'styled-components';

import { AssertionGroupHeader } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AssertionGroupHeader';
import {
    ASSERTION_DEFAULT_RAW_DATA,
    ASSERTION_SOURCES,
} from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/constant';
import {
    AssertionColumnGroup,
    AssertionFilterOptions,
    AssertionListFilter,
    AssertionListTableRow,
    AssertionRecommendedFilter,
    AssertionStatusGroup,
    AssertionTable,
    AssertionWithDescription,
} from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { AssertionGroup } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylTypes';
import {
    ASSERTION_INFO,
    createAssertionGroups,
    getAssertionGroupName,
    getAssertionType,
} from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { isExternalAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/isExternalAssertion';
import { getPlainTextDescriptionFromAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/utils';
import { getCustomAssertionFields } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/shared/structuredAssertionUtils';
import {
    ASSERTION_FIELD_PATH_FILTER_NAME,
    ASSERTION_SOURCE_FILTER_NAME,
    ASSERTION_STATUS_FILTER_NAME,
    ASSERTION_TYPE_FILTER_NAME,
    OWNERS_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import {
    Assertion,
    AssertionInfo,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AssertionSourceType,
    AssertionType,
    AuditStamp,
    EntityType,
    FacetMetadata,
    Tag,
    TagAssociation,
} from '@src/types.generated';

const ASSERTION_TYPE_NAME_MAP = {
    get VOLUME() {
        return i18next.t('entity.profile.validations:assertionType.volume');
    },
    get SQL() {
        return i18next.t('entity.profile.validations:assertionType.sql');
    },
    get FIELD() {
        return i18next.t('entity.profile.validations:assertionType.column');
    },
    get FRESHNESS() {
        return i18next.t('entity.profile.validations:assertionType.freshness');
    },
    get DATASET() {
        return i18next.t('entity.profile.validations:assertionType.other');
    },
    get DATA_SCHEMA() {
        return i18next.t('entity.profile.validations:assertionType.schema');
    },
    get Unknown() {
        return i18next.t('entity.profile.validations:assertionType.unknown');
    },
};
const NO_STATUS = 'NO_STATUS';

const ASSERTION_STATUS_NAME_MAP = {
    get FAILURE() {
        return i18next.t('entity.profile.validations:status.failing');
    },
    get SUCCESS() {
        return i18next.t('entity.profile.validations:status.passing');
    },
    get ERROR() {
        return i18next.t('entity.profile.validations:status.error');
    },
    get INIT() {
        return i18next.t('entity.profile.validations:status.initializing');
    },
    get [NO_STATUS]() {
        return i18next.t('entity.profile.validations:status.noStatus');
    },
};

const getStatusGroupDisplayName = (name: string): string =>
    ASSERTION_STATUS_NAME_MAP[name] || ASSERTION_TYPE_NAME_MAP[name] || name;

const RECOMMENDED_FILTER_NAME_MAP = {
    get [AssertionSourceType.External]() {
        return i18next.t('entity.profile.validations:sourceType.external');
    },
};

// Create Group's Summary to name and number of records for each group
const getGroupNameBySummary = (record) => {
    const TextContainer = styled.div`
        display: flex;
        align-items: center;
        justify-content: left;
        font-size: 14px;
    `;

    const Title = styled(Text)`
        && {
            padding-bottom: 0px;
            margin-bottom: 0px;
        }
    `;

    const Message = styled(Text)`
        && {
            font-size: 12px;
            margin-left: 8px;
        }
    `;

    const newSummary = record.summary;
    const list: string[] = [];
    Object.keys(newSummary).forEach((key) => {
        if (newSummary[key] > 0) {
            list.push(`${newSummary[key]} ${getStatusGroupDisplayName(key)}`);
        }
    });

    return (
        <TextContainer>
            <Title type="span" weight="bold">
                {getStatusGroupDisplayName(record.name)}
            </Title>
            <Message type="span" color="textSecondary">
                {i18next.t('entity.profile.validations:assertionList.groupHeaderSummaryListTemplate', {
                    listItems: list,
                })}
            </Message>
        </TextContainer>
    );
};

// transform assertions into table data
export const mapAssertionDataToTableProperties = (assertions: Assertion[]): AssertionListTableRow[] => {
    return assertions.map((assertion: Assertion) => {
        const mostRecentRun = assertion.runEvents?.runEvents?.[0];

        const primaryPainTextLabel = getPlainTextDescriptionFromAssertion(assertion.info as AssertionInfo);
        const isCompleted = mostRecentRun?.status === AssertionRunStatus.Complete;
        const rowData: AssertionListTableRow = {
            key: assertion.urn,
            type: getAssertionType(assertion),
            lastUpdated: assertion.info?.lastUpdated as AuditStamp,
            tags: assertion.tags?.tags as TagAssociation[],
            descriptionHTML: null,
            description: primaryPainTextLabel,
            urn: assertion.urn,
            platform: assertion.platform,
            lastEvaluation: (isCompleted && mostRecentRun) as AssertionRunEvent,
            lastEvaluationTimeMs: mostRecentRun?.timestampMillis,
            lastEvaluationResult: (isCompleted && mostRecentRun?.result?.type) as AssertionResultType,
            lastEvaluationUrl: (isCompleted && mostRecentRun?.result?.externalUrl) || '',
            assertion: assertion as Assertion,
            ownership: assertion.ownership,
            status: mostRecentRun?.status as AssertionRunStatus,
        };
        return rowData;
    });
};

const CORE_STATUSES = [AssertionResultType.Failure, AssertionResultType.Error, AssertionResultType.Success];

// Generate Assertion Group By Status
const generateAssertionGroupByStatus = (assertions: Assertion[]): AssertionStatusGroup[] => {
    const assertionStatus = [...CORE_STATUSES, AssertionResultType.Init, NO_STATUS];

    const assertionGroup: AssertionStatusGroup[] = [];

    assertionStatus.forEach((status) => {
        const filteredAssertions = assertions.filter((assertion) => {
            const mostRecentRun = assertion.runEvents?.runEvents?.[0];
            const resultType = mostRecentRun?.result?.type;
            if (status === NO_STATUS) {
                return assertion.info?.type && resultType === undefined;
            }
            return assertion.info?.type && resultType === status;
        });

        if (filteredAssertions.length > 0) {
            const summary = {};
            filteredAssertions.forEach((assertion) => {
                const assertionType = getAssertionType(assertion) || 'Unknown';
                summary[assertionType] = (summary[assertionType] || 0) + 1;
            });
            const group: AssertionStatusGroup = {
                name: status,
                assertions: mapAssertionDataToTableProperties(filteredAssertions),
                summary,
            };
            assertionGroup.push({ ...group, groupName: getGroupNameBySummary(group) });
        }
    });

    return assertionGroup;
};

// get Assertion group by Display order
export const getAssertionGroupsByDisplayOrder = (assertionGroups: AssertionGroup[]) => {
    // Create a map of order from the display order
    const orderMap = new Map(ASSERTION_INFO.map((item, index) => [item.type, index]));

    // Sort the unordered list based on the orderMap
    const orderedAssertionGroups = assertionGroups.sort(
        (a, b) => (orderMap.get(a.type) ?? Infinity) - (orderMap.get(b.type) ?? Infinity),
    );
    return orderedAssertionGroups;
};

// Build the Filter Options as per the type & status
const buildFilterOptions = (
    key: keyof AssertionFilterOptions['filterGroupOptions'],
    value: Record<string, number>,
    filterOptions: AssertionFilterOptions,
    displayNames: Record<string, string> = {},
) => {
    Object.entries(value).forEach(([name, count]) => {
        let displayName =
            displayNames[name] || (key === 'type' ? getAssertionGroupName(name) : getStatusGroupDisplayName(name));
        if (key === 'source') {
            displayName = RECOMMENDED_FILTER_NAME_MAP[name];
        }
        const filterItem: AssertionRecommendedFilter = { name, category: key, count, displayName };

        filterOptions.recommendedFilters.push(filterItem);
        filterOptions.filterGroupOptions[key].push(filterItem);
    });
};

// Column paths targeted by Field / Custom / legacy Dataset assertions.
export const getColumnIdsFromAssertion = (assertion: Assertion): string[] => {
    const info = assertion?.info;

    if (info?.type === AssertionType.Field) {
        const fieldAssertion = info?.fieldAssertion;
        const field = (fieldAssertion?.fieldMetricAssertion || fieldAssertion?.fieldValuesAssertion)?.field;
        return field?.path ? [field.path] : [];
    }

    if (info?.type === AssertionType.Custom) {
        return getCustomAssertionFields(info.customAssertion)
            .map((f) => f.path)
            .filter((path): path is string => Boolean(path));
    }

    if (info?.type === AssertionType.Dataset && info.datasetAssertion?.fields?.length) {
        return info.datasetAssertion.fields.map((f) => f.path).filter((path): path is string => Boolean(path));
    }

    return [];
};

/** Create filter option list as per the assertion data present 
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
const extractFilterOptionListFromAssertions = (assertions: Assertion[]) => {
    const filterOptions: AssertionFilterOptions = {
        filterGroupOptions: {
            type: [],
            status: [],
            column: [],
            tags: [],
            source: [],
            owners: [],
        },
        recommendedFilters: [],
    };

    const filterGroupCounts = {
        type: {} as Record<string, number>,
        status: {} as Record<string, number>,
        column: {} as Record<string, number>,
        tags: {} as Record<string, number>,
        source: {} as Record<string, number>,
    };
    const tagDisplayNames: Record<string, string> = {};

    // maintain array to show all the Assertion Type count even if it is not present
    const remainingAssertionTypes = ASSERTION_INFO.map((item) => item.type);
    const remainingAssertionStatus = [...CORE_STATUSES];
    const remainingAssertionSources = [...ASSERTION_SOURCES];

    assertions.forEach((assertion: Assertion) => {
        // filter out tracked types
        const type = (getAssertionType(assertion) || '') as AssertionType;
        const index = remainingAssertionTypes.indexOf(type);
        if (index > -1) {
            remainingAssertionTypes.splice(index, 1);
        }

        filterGroupCounts.type[type] = (filterGroupCounts.type[type] || 0) + 1;

        // getAssertionType prefers customAssertion.type (e.g. GREAT_EXPECTATIONS). When that
        // differs from CUSTOM, also count the Custom ASSERTION_INFO bucket. Skip when type is
        // already CUSTOM to avoid double-counting subtype-less customs.
        if (assertion.info?.type === AssertionType.Custom) {
            if (type !== AssertionType.Custom) {
                filterGroupCounts.type[AssertionType.Custom] = (filterGroupCounts.type[AssertionType.Custom] || 0) + 1;
            }
            const customIndex = remainingAssertionTypes.indexOf(AssertionType.Custom);
            if (customIndex > -1) {
                remainingAssertionTypes.splice(customIndex, 1);
            }
        }

        // filter out tracked statuses
        const mostRecentRun = assertion.runEvents?.runEvents?.[0];
        const resultType = mostRecentRun?.result?.type || '';
        if (resultType) {
            const statusIndex = remainingAssertionStatus.indexOf(resultType);
            if (statusIndex > -1) {
                remainingAssertionStatus.splice(statusIndex, 1);
            }

            filterGroupCounts.status[resultType] = (filterGroupCounts.status[resultType] || 0) + 1;
        }

        const tags = assertion.tags?.tags || [];
        tags.forEach((tag) => {
            const tagUrn = tag.tag.urn;
            if (tagUrn) {
                filterGroupCounts.tags[tagUrn] = (filterGroupCounts.tags[tagUrn] || 0) + 1;
                tagDisplayNames[tagUrn] = tag.tag.properties?.name || tag.tag.name || tagUrn;
            }
        });

        // count columnIds assertion - handles multi-column Custom assertions
        getColumnIdsFromAssertion(assertion).forEach((columnId) => {
            filterGroupCounts.column[columnId] = (filterGroupCounts.column[columnId] || 0) + 1;
        });

        // count source type assertion
        let sourceType = assertion.info?.source?.type as AssertionSourceType;
        if (isExternalAssertion(assertion)) {
            filterGroupCounts.source[AssertionSourceType.External] =
                (filterGroupCounts.source[AssertionSourceType.External] || 0) + 1;
            sourceType = AssertionSourceType.External;
        } else {
            filterGroupCounts.source[sourceType] = (filterGroupCounts.source[sourceType] || 0) + 1;
        }
        const sourceTypeIndex = remainingAssertionSources.indexOf(sourceType);
        if (sourceTypeIndex > -1) {
            remainingAssertionSources.splice(sourceTypeIndex, 1);
        }
    });

    // Add remaining Assertion type with count 0
    remainingAssertionTypes.forEach((assertionType: AssertionType) => {
        filterGroupCounts.type[assertionType] = 0;
    });

    // Add remaining Assertion status with count 0
    remainingAssertionStatus.forEach((status: AssertionResultType) => {
        filterGroupCounts.status[status] = 0;
    });

    // Add remaining Assertion status with count 0
    remainingAssertionSources.forEach((sourceType: AssertionSourceType) => {
        filterGroupCounts.source[sourceType] = 0;
    });

    buildFilterOptions('status', filterGroupCounts.status, filterOptions);
    buildFilterOptions('type', filterGroupCounts.type, filterOptions);
    buildFilterOptions('column', filterGroupCounts.column, filterOptions);
    buildFilterOptions('source', filterGroupCounts.source, filterOptions);
    buildFilterOptions('tags', filterGroupCounts.tags, filterOptions, tagDisplayNames);
    return filterOptions;
};

const normalizeStatusFacet = (value: string): string => {
    if (value === 'PASSING') return AssertionResultType.Success;
    if (value === 'FAILING') return AssertionResultType.Failure;
    return value;
};

export const extractFilterOptionsFromFacets = (
    assertions: Assertion[],
    facets?: FacetMetadata[],
): AssertionFilterOptions => {
    if (!facets) {
        return extractFilterOptionListFromAssertions(assertions);
    }

    const filterOptions: AssertionFilterOptions = {
        filterGroupOptions: {
            type: [],
            status: [],
            column: [],
            tags: [],
            source: [],
            owners: [],
        },
        recommendedFilters: [],
    };
    const fields: Array<{ field: string; category: keyof AssertionFilterOptions['filterGroupOptions'] }> = [
        { field: ASSERTION_STATUS_FILTER_NAME, category: 'status' },
        { field: ASSERTION_TYPE_FILTER_NAME, category: 'type' },
        { field: ASSERTION_FIELD_PATH_FILTER_NAME, category: 'column' },
        { field: ASSERTION_SOURCE_FILTER_NAME, category: 'source' },
        { field: TAGS_FILTER_NAME, category: 'tags' },
        { field: OWNERS_FILTER_NAME, category: 'owners' },
    ];

    fields.forEach(({ field, category }) => {
        const facet = facets.find((item) => item.field === field);
        const counts: Record<string, number> = {};
        const displayNames: Record<string, string> = {};
        facet?.aggregations?.forEach((aggregation) => {
            if (!aggregation.value) return;
            const value = category === 'status' ? normalizeStatusFacet(aggregation.value) : aggregation.value;
            counts[value] = aggregation.count || 0;
            if (category === 'tags' && aggregation.entity?.type === EntityType.Tag) {
                const tag = aggregation.entity as Tag;
                displayNames[value] = tag.properties?.name || tag.name || value;
            } else if (category === 'owners' && aggregation.entity) {
                const owner = aggregation.entity as typeof aggregation.entity & {
                    info?: { displayName?: string; fullName?: string };
                    properties?: { displayName?: string };
                    username?: string;
                    name?: string;
                };
                displayNames[value] =
                    owner.info?.displayName ||
                    owner.info?.fullName ||
                    owner.properties?.displayName ||
                    owner.username ||
                    owner.name ||
                    value;
            }
        });
        buildFilterOptions(category, counts, filterOptions, displayNames);
    });

    ASSERTION_INFO.map((item) => item.type)
        .filter((type) => !filterOptions.filterGroupOptions.type.some((item) => item.name === type))
        .forEach((type) => buildFilterOptions('type', { [type]: 0 }, filterOptions));
    CORE_STATUSES.filter(
        (status) => !filterOptions.filterGroupOptions.status.some((item) => item.name === status),
    ).forEach((status) => buildFilterOptions('status', { [status]: 0 }, filterOptions));
    ASSERTION_SOURCES.filter(
        (source) => !filterOptions.filterGroupOptions.source.some((item) => item.name === source),
    ).forEach((source) => buildFilterOptions('source', { [source]: 0 }, filterOptions));

    return filterOptions;
};

// create column id group from column assertions
const groupColumnAssertions = (assertions: Assertion[]): AssertionColumnGroup[] => {
    const columnIdGroups: AssertionColumnGroup[] = [];
    const columnIdToAssertionMap = new Map<string, Assertion[]>();
    assertions.forEach((assertion: Assertion) => {
        // Add assertion to each column group it belongs to (multi-column Custom assertions
        // appear under every referenced column).
        getColumnIdsFromAssertion(assertion).forEach((columnId) => {
            const columnAssertions = columnIdToAssertionMap.get(columnId) || [];
            columnAssertions.push(assertion);
            columnIdToAssertionMap.set(columnId, columnAssertions);
        });
    });

    // transform columnIds group data into table render Row
    columnIdToAssertionMap.forEach((columnAssertions: Assertion[], columnId: string) => {
        const assertionColumnGroup: AssertionColumnGroup = {
            name: columnId,
            assertions: mapAssertionDataToTableProperties(columnAssertions),
        };
        columnIdGroups.push(assertionColumnGroup);
    });
    return columnIdGroups;
};

// Assign Filtered Assertions to group
const assignFilteredAssertionToGroup = (filteredAssertions: AssertionWithDescription[]): AssertionTable => {
    const assertionRawData: AssertionTable = {
        ...ASSERTION_DEFAULT_RAW_DATA,
    };
    assertionRawData.assertions = mapAssertionDataToTableProperties(filteredAssertions);
    const assertionsByType = createAssertionGroups(filteredAssertions);
    assertionRawData.groupBy.type = getAssertionGroupsByDisplayOrder(assertionsByType);
    // Column grouping: Field, Custom, and legacy Dataset assertions that target columns
    const columnTypeAssertions = filteredAssertions.filter(
        (assertion) => getColumnIdsFromAssertion(assertion).length > 0,
    );

    assertionRawData.groupBy.type?.forEach((item) => {
        const transformedData = mapAssertionDataToTableProperties(item.assertions);
        // eslint-disable-next-line  no-param-reassign
        item.assertions = transformedData;
        // eslint-disable-next-line  no-param-reassign
        item.groupName = <AssertionGroupHeader group={item} />;
    });
    assertionRawData.groupBy.status = generateAssertionGroupByStatus(filteredAssertions);
    const columnsGroup = groupColumnAssertions(columnTypeAssertions);
    assertionRawData.groupBy.column = columnsGroup;
    assertionRawData.filterOptions = extractFilterOptionListFromAssertions(filteredAssertions);
    return assertionRawData;
};

/**
 * Type filter matching. `getAssertionType` prefers `customAssertion.type` (provider key),
 * so selecting Custom must also match any assertion whose top-level type is CUSTOM.
 */
export const assertionMatchesTypeFilter = (assertion: Assertion, selectedTypes: AssertionType[]): boolean => {
    if (selectedTypes.length === 0) {
        return true;
    }
    const resolvedType = getAssertionType(assertion) as AssertionType;
    if (selectedTypes.includes(resolvedType)) {
        return true;
    }
    return selectedTypes.includes(AssertionType.Custom) && assertion.info?.type === AssertionType.Custom;
};

const getFilteredAssertions = (assertions: AssertionWithDescription[], filter: AssertionListFilter) => {
    const { type, status, source, column } = filter.filterCriteria;

    // Apply type, status, and other filters
    return assertions.filter((assertion: Assertion) => {
        const resultType = assertion.runEvents?.runEvents?.[0]?.result?.type as AssertionResultType;
        const columnIds = getColumnIdsFromAssertion(assertion);
        const matchesType = assertionMatchesTypeFilter(assertion, type);
        const matchesStatus = status.length === 0 || status.includes(resultType);
        // Match if ANY of the assertion's columns is in the filter
        const matchesColumn = column.length === 0 || columnIds.some((id) => column.includes(id));
        const matchesOthers =
            source.length === 0 ||
            source.includes(assertion.info?.source?.type as AssertionSourceType) ||
            (source.includes(AssertionSourceType.External) && isExternalAssertion(assertion));

        return matchesType && matchesStatus && matchesOthers && matchesColumn;
    });
};

// Fuse.js setup for search functionality
const fuse = new Fuse<AssertionWithDescription>([], {
    keys: ['description'],
    threshold: 0.4,
});

/** Return filter assertion as per selected type status and other things
 * it returns transformated into
 * 1. group of assertions as per type , status
 * 2. Transform data into {@link AssertionListTableRow }  data
 * 2. Filter out assertions as per the search text
 * 3. filter out assertions as per the selected type and status
 */
export const getFilteredTransformedAssertionData = (
    assertions: Assertion[],
    filter: AssertionListFilter,
): AssertionTable => {
    // Add descriptions to assertions
    const assertionsWithDescription = assertions.map((assertion) => {
        const description = getPlainTextDescriptionFromAssertion(assertion.info as AssertionInfo);
        return {
            ...assertion,
            description,
        };
    });

    // Apply search filter if searchText is provided
    let filteredAssertions = assertionsWithDescription;
    const { searchText } = filter.filterCriteria;
    let searchMatchesCount = 0;
    if (searchText) {
        fuse.setCollection(assertionsWithDescription || []);
        const result = fuse.search(searchText);
        filteredAssertions = result.map((match) => match.item as AssertionWithDescription);
        searchMatchesCount = filteredAssertions.length;
    }

    // Apply type, status, and other filters
    filteredAssertions = getFilteredAssertions(filteredAssertions, filter);

    // Transform filtered assertions
    const assertionRawData = assignFilteredAssertionToGroup(filteredAssertions);
    assertionRawData.totalCount = assertions.length;
    assertionRawData.searchMatchesCount = searchMatchesCount;
    assertionRawData.filteredCount = getFilteredAssertions(assertionsWithDescription, filter).length;
    assertionRawData.originalFilterOptions = extractFilterOptionListFromAssertions(assertions);
    return assertionRawData;
};

/** Build the Assertion Redirect Search Param URL to help add with location pathname for redirection */
export const buildAssertionUrlSearch = ({
    type,
    status,
}: {
    type?: AssertionType;
    status?: AssertionResultType;
}): string => {
    const { search } = window.location;
    const params = new URLSearchParams(search);

    if (type) {
        params.set('assertion_type', type);
    }
    if (status) {
        params.set('assertion_status', status);
    }

    return params.toString() ? `?${params.toString()}` : '';
};
