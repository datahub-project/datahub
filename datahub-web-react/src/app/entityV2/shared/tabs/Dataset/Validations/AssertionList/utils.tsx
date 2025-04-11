import React from 'react';
import Fuse from 'fuse.js';
import styled from 'styled-components';
import { Typography } from 'antd';
import {
    Assertion,
    AssertionInfo,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AssertionSourceType,
    AssertionType,
    AuditStamp,
    TagAssociation,
    EntityType,
} from '@src/types.generated';
import { AssertionGroup } from '@src/app/entity/shared/tabs/Dataset/Validations/acrylTypes';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { getPlatformName } from '@src/app/entityV2/shared/utils';
import { ASSERTION_INFO, createAssertionGroups, getAssertionGroupName, getAssertionType } from '../acrylUtils';
import { isExternalAssertion } from '../assertion/profile/shared/isExternalAssertion';
import { AssertionGroupHeader } from './AssertionGroupHeader';
import {
    AssertionStatusGroup,
    AssertionTable,
    AssertionListFilter,
    AssertionListTableRow,
    AssertionFilterOptions,
    AssertionRecommendedFilter,
    AssertionWithDescription,
    AssertionColumnGroup,
    AssertionBuilderSiblingOptions,
} from './types';
import { ASSERTION_DEFAULT_RAW_DATA, ASSERTION_SOURCES } from './constant';
import { getPlainTextDescriptionFromAssertion } from '../assertion/profile/summary/utils';

const ASSERTION_TYPE_NAME_MAP = {
    VOLUME: 'Volume',
    SQL: 'Sql',
    FIELD: 'Column',
    FRESHNESS: 'Freshness',
    DATASET: 'Other',
    DATA_SCHEMA: 'Schema',
    Unknown: 'Unknown',
};
const NO_STATUS = 'NO_STATUS';

const ASSERTION_STATUS_NAME_MAP = {
    FAILURE: 'Failing',
    SUCCESS: 'Passing',
    ERROR: 'Error',
    [NO_STATUS]: 'No Status',
};

const STATUS_GROUP_NAME_MAP = { ...ASSERTION_TYPE_NAME_MAP, ...ASSERTION_STATUS_NAME_MAP };

const RECOMMENDED_FILTER_NAME_MAP = {
    [AssertionSourceType.External]: 'External',
    [AssertionSourceType.Native]: 'Native',
    [AssertionSourceType.Inferred]: 'Smart Assertions',
};

// Create Group's Summary to name and number of records for each group
const getGroupNameBySummary = (record) => {
    const TextContainer = styled.div`
        display: flex;
        align-items: center;
        justify-content: left;
        font-size: 14px;
    `;

    const Title = styled(Typography.Text)`
        && {
            padding-bottom: 0px;
            margin-bottom: 0px;
        }
    `;

    const Message = styled(Typography.Text)`
        && {
            font-size: 12px;
            margin-left: 8px;
        }
    `;

    const newSummary = record.summary;
    const list: string[] = [];
    Object.keys(newSummary).forEach((key) => {
        if (newSummary[key] > 0) {
            list.push(`${newSummary[key]} ${STATUS_GROUP_NAME_MAP[key]}`);
        }
    });

    return (
        <TextContainer>
            <Title strong>{STATUS_GROUP_NAME_MAP[record.name]}</Title>
            <Message type="secondary">{list.join(', ')}</Message>
        </TextContainer>
    );
};

/**
 * Gets sibling options that a user can author assertions with
 * This includes direct links that will open the respective siblings' assertion builder UI
 * @param entityData
 * @param urn
 * @param entityType
 * @returns {AssertionBuilderSiblingOptions[]}
 */
export const useSiblingOptionsForAssertionBuilder = (
    entityData: GenericEntityProperties | null,
    urn: string,
    entityType: EntityType,
): AssertionBuilderSiblingOptions[] => {
    const optionsToAuthorOn: AssertionBuilderSiblingOptions[] = [];
    // push main entity data
    optionsToAuthorOn.push({
        title:
            entityData?.platform?.properties?.displayName ??
            entityData?.platform?.name ??
            entityData?.dataPlatformInstance?.platform.name ??
            entityData?.platform?.urn ??
            urn,
        disabled: true,
        urn,
        platform: entityData?.platform ?? entityData?.dataPlatformInstance?.platform,
        entityType,
    });
    // push siblings data
    const siblings: GenericEntityProperties[] = entityData?.siblingsSearch?.searchResults?.map((r) => r.entity) || [];
    siblings.forEach((sibling) => {
        if (sibling.urn === urn || !sibling.urn) {
            return;
        }
        optionsToAuthorOn.push({
            urn: sibling.urn,
            title:
                getPlatformName(sibling) ??
                sibling?.dataPlatformInstance?.platform?.name ??
                sibling?.platform?.urn ??
                sibling.urn,
            disabled: true,
            platform: sibling?.platform ?? sibling?.dataPlatformInstance?.platform,
            entityType: sibling.type,
        });
    });
    return optionsToAuthorOn;
};

// transform assertions into table data
const mapAssertionData = (assertions: Assertion[]): AssertionListTableRow[] => {
    return assertions.map((assertion: Assertion) => {
        const mostRecentRun = assertion.runEvents?.runEvents?.[0];

        const primaryPainTextLabel = getPlainTextDescriptionFromAssertion(assertion.info as AssertionInfo);
        const isCompleted = mostRecentRun?.status === AssertionRunStatus.Complete;
        const rowData: AssertionListTableRow = {
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
                assertions: mapAssertionData(filteredAssertions),
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
const buildFilterOptions = (key: string, value: Record<string, number>, filterOptions: AssertionFilterOptions) => {
    Object.entries(value).forEach(([name, count]) => {
        let displayName = key === 'type' ? getAssertionGroupName(name) : STATUS_GROUP_NAME_MAP[name] || name;
        if (key === 'source') {
            displayName = RECOMMENDED_FILTER_NAME_MAP[name];
        }
        const filterItem = { name, category: key, count, displayName } as AssertionRecommendedFilter;

        filterOptions.recommendedFilters.push(filterItem);
        filterOptions.filterGroupOptions[key].push(filterItem);
    });
};

// get columnId from Column/Field Assertion
const getColumnIdFromAssertion = (assertion: Assertion): string | null => {
    const info = assertion?.info;
    const fieldAssertion = info?.fieldAssertion;
    if (info?.type === AssertionType.Field) {
        const field = (fieldAssertion?.fieldMetricAssertion || fieldAssertion?.fieldValuesAssertion)?.field;
        return field?.path || null;
    }
    return null;
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
            const tagName = tag.tag.properties?.name || '';
            filterGroupCounts.tags[tagName] = (filterGroupCounts.tags[tagName] || 0) + 1;
        });

        // count columnIds assertion
        const columnId = getColumnIdFromAssertion(assertion);
        if (columnId) {
            filterGroupCounts.column[columnId] = (filterGroupCounts.column[columnId] || 0) + 1;
        }

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
            remainingAssertionSources.splice(index, 1);
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
    return filterOptions;
};

// create column id group from column assertions
const groupColumnAssertions = (assertions: Assertion[]): AssertionColumnGroup[] => {
    const columnIdGroups: AssertionColumnGroup[] = [];
    const columnIdToAssertionMap = new Map<string, Assertion[]>();
    assertions.forEach((assertion: Assertion) => {
        const columnId = getColumnIdFromAssertion(assertion);
        if (columnId) {
            const columnAssertions = columnIdToAssertionMap.get(columnId) || [];
            columnAssertions.push(assertion);
            columnIdToAssertionMap.set(columnId, columnAssertions);
        }
    });

    // transform columnIds group data into table render Row
    columnIdToAssertionMap.forEach((columnAssertions: Assertion[], columnId: string) => {
        const assertionColumnGroup: AssertionColumnGroup = {
            name: columnId,
            assertions: mapAssertionData(columnAssertions),
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
    assertionRawData.assertions = mapAssertionData(filteredAssertions);
    const assertionsByType = createAssertionGroups(filteredAssertions);
    assertionRawData.groupBy.type = getAssertionGroupsByDisplayOrder(assertionsByType);
    // separate out column assertion list for filter
    const columnTypeAssertions =
        assertionRawData.groupBy.type?.find((item) => item.type === AssertionType.Field)?.assertions || [];

    assertionRawData.groupBy.type?.forEach((item) => {
        const transformedData = mapAssertionData(item.assertions);
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

const getFilteredAssertions = (assertions: AssertionWithDescription[], filter: AssertionListFilter) => {
    const { type, status, source, column } = filter.filterCriteria;

    // Apply type, status, and other filters
    return assertions.filter((assertion: Assertion) => {
        const resultType = assertion.runEvents?.runEvents?.[0]?.result?.type as AssertionResultType;
        const columnId = getColumnIdFromAssertion(assertion) || '';
        const matchesType = type.length === 0 || type.includes(getAssertionType(assertion) as AssertionType);
        const matchesStatus = status.length === 0 || status.includes(resultType);
        const matchesColumn = column.length === 0 || column.includes(columnId);
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

/** Return return filter assertion as per selected type status and other things
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
