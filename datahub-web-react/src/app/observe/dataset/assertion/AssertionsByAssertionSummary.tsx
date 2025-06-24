import { SimpleSelect, Tooltip, colors } from '@components';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { FilterSelect } from '@app/entityV2/shared/FilterSelect';
import { InlineListSearch } from '@app/entityV2/shared/components/search/InlineListSearch';
import { AssertionsByAssertionSummaryTable } from '@app/observe/dataset/assertion/AssertionsByAssertionSummaryTable';
import {
    ASSERTION_RESULT_TYPE_OPTIONS_TO_RUN_SUMMARY_FILTER_FIELD,
    ASSERTION_TYPE_OPTIONS,
    AssertionResultTypeOptions,
    LAST_ASSERTION_RUN_AT_SORT_FIELD,
    RUN_EVENTS_PREVIEW_LIMIT,
} from '@app/observe/dataset/assertion/constants';
import { compareListItems } from '@app/observe/dataset/assertion/util';
import { Header } from '@app/observe/dataset/shared/shared';
import BaseEntityFilter from '@app/searchV2/filtersV2/filters/BaseEntityFilter/BaseEntityFilter';
import {
    ASSERTEE_DOMAINS_FILTER_NAME,
    ASSERTEE_GLOSSARY_TERMS_FILTER_NAME,
    ASSERTEE_OWNERS_FILTER_NAME,
    ASSERTEE_PLATFORM_FILTER_NAME,
    ASSERTEE_TAGS_FILTER_NAME,
    ASSERTION_TYPE_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useSearchAssertionsQuery } from '@graphql/monitor.generated';
import {
    AndFilterInput,
    Assertion,
    Dataset,
    Entity,
    EntityType,
    FacetFilterInput,
    FilterOperator,
    Maybe,
    Monitor,
    SortOrder,
} from '@types';

const VerticalDivider = styled.div`
    width: 1px;
    height: 36px;
    background-color: ${colors.gray[200]};
`;

const Container = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: hidden;
    height: 100%;
`;

const FilterOptionsWrapper = styled.div`
    display: flex;
    gap: 12px;
    align-items: center;
`;

const DEFAULT_PAGE_SIZE = 25;

const DEFAULT_STATUS_OPTIONS: AssertionResultTypeOptions[] = ['Failing', 'Error', 'Passing'];

const STATUS_OPTIONS_TO_LABEL: Record<AssertionResultTypeOptions, string> = {
    Failing: 'At least one failure',
    Error: 'At least one error',
    Passing: 'At least one success',
};

type TimeRange = {
    start: number;
    end: number;
    label: 'Last 24 hours' | 'Last 7 days' | 'Last 30 days' | 'Last 1 year';
};

const getTimeRangeOptions = (): TimeRange[] => {
    const now = new Date().getTime();
    const oneDayAgo = new Date(now - 1 * 24 * 60 * 60 * 1000);
    const oneWeekAgo = new Date(now - 7 * 24 * 60 * 60 * 1000);
    const oneMonthAgo = new Date(now - 30 * 24 * 60 * 60 * 1000);
    const oneYearAgo = new Date(now - 365 * 24 * 60 * 60 * 1000);
    return [
        { start: oneDayAgo.getTime(), end: now, label: 'Last 24 hours' },
        { start: oneWeekAgo.getTime(), end: now, label: 'Last 7 days' },
        { start: oneMonthAgo.getTime(), end: now, label: 'Last 30 days' },
        { start: oneYearAgo.getTime(), end: now, label: 'Last 1 year' },
    ];
};

type AssetFilterOptions = {
    platform: string[];
    domain: string[];
    owner: string[];
    term: string[];
    tag: string[];
};

const DEFAULT_TIME_RANGE_LABEL: TimeRange['label'] = 'Last 7 days';

const getDefaultTimeRange = (timeRangeOptions: TimeRange[]): TimeRange => {
    return timeRangeOptions.find((option) => option.label === DEFAULT_TIME_RANGE_LABEL) || timeRangeOptions[0];
};

export const AssertionsByAssertionSummary = () => {
    const entityRegistry = useEntityRegistry();
    const tryGetDisplayName = (entity?: Maybe<Entity>): string | undefined => {
        if (!entity) {
            return undefined;
        }
        try {
            return entityRegistry.getDisplayName(entity.type, entity);
        } catch (error) {
            return undefined;
        }
    };

    const timeRangeOptions = getTimeRangeOptions();

    // pagination
    const [page, setPage] = useState(1);
    const start = (page - 1) * DEFAULT_PAGE_SIZE;

    // together these two filters are used to filter assertions by status and time range
    // i.e., failed assertions in the last 7 days
    const [statuses, setStatuses] = useState<AssertionResultTypeOptions[]>(DEFAULT_STATUS_OPTIONS);
    const [timeRange, setTimeRange] = useState<TimeRange>(getDefaultTimeRange(timeRangeOptions));

    // Filters
    const [searchQuery, setSearchQuery] = useState('');
    const [assertionTypes, setAssertionTypes] = useState<string[]>(
        ASSERTION_TYPE_OPTIONS.map((option) => option.value),
    );
    const [assertionTags, setAssertionTags] = useState<string[]>([]);

    // Asset Filters
    const [assetFilterOptions, setAssetFilterOptions] = useState<AssetFilterOptions>({
        platform: [],
        domain: [],
        owner: [],
        term: [],
        tag: [],
    });

    // Reset page when filters change
    useEffect(() => {
        setPage(1);
    }, [searchQuery, statuses, assertionTypes, assertionTags, assetFilterOptions]);

    // Has Filters
    const hasFilters =
        statuses.length !== DEFAULT_STATUS_OPTIONS.length ||
        searchQuery.length > 0 ||
        assertionTypes.length > 0 ||
        assertionTags.length > 0 ||
        Object.values(assetFilterOptions).some((value) => value.length > 0);

    const orFilters: AndFilterInput[] = [];
    statuses.forEach((status) => {
        const filters: FacetFilterInput[] = [];
        filters.push({
            field: ASSERTION_RESULT_TYPE_OPTIONS_TO_RUN_SUMMARY_FILTER_FIELD[status],
            values: [timeRange.start.toString(), timeRange.end.toString()],
            condition: FilterOperator.Between,
        });

        filters.push({ field: ASSERTION_TYPE_FILTER_NAME, values: assertionTypes });

        if (assertionTags.length > 0) {
            filters.push({ field: TAGS_FILTER_NAME, values: assertionTags });
        }

        if (assetFilterOptions.platform.length > 0) {
            filters.push({
                field: ASSERTEE_PLATFORM_FILTER_NAME,
                values: assetFilterOptions.platform,
                condition: FilterOperator.In,
            });
        }

        if (assetFilterOptions.domain.length > 0) {
            filters.push({
                field: ASSERTEE_DOMAINS_FILTER_NAME,
                values: assetFilterOptions.domain,
                condition: FilterOperator.In,
            });
        }

        if (assetFilterOptions.owner.length > 0) {
            filters.push({
                field: ASSERTEE_OWNERS_FILTER_NAME,
                values: assetFilterOptions.owner,
                condition: FilterOperator.In,
            });
        }

        if (assetFilterOptions.term.length > 0) {
            filters.push({
                field: ASSERTEE_GLOSSARY_TERMS_FILTER_NAME,
                values: assetFilterOptions.term,
                condition: FilterOperator.In,
            });
        }

        if (assetFilterOptions.tag.length > 0) {
            filters.push({
                field: ASSERTEE_TAGS_FILTER_NAME,
                values: assetFilterOptions.tag,
                condition: FilterOperator.In,
            });
        }

        orFilters.push({ and: filters });
    });

    const { data: searchResults, loading } = useSearchAssertionsQuery({
        variables: {
            input: {
                types: [EntityType.Assertion],
                query: searchQuery || '*',
                start,
                count: DEFAULT_PAGE_SIZE,
                orFilters,
                sortInput: {
                    sortCriterion: {
                        field: LAST_ASSERTION_RUN_AT_SORT_FIELD,
                        sortOrder: SortOrder.Descending,
                    },
                },
            },
            runEventsStart: timeRange.start,
            runEventsEnd: timeRange.end,
            runEventsLimit: RUN_EVENTS_PREVIEW_LIMIT,
        },
    });

    const total = searchResults?.searchAcrossEntities?.total ?? 0;
    const facets = searchResults?.searchAcrossEntities?.facets;

    const assertions: Assertion[] =
        searchResults?.searchAcrossEntities?.searchResults?.map(
            (result) =>
                ({
                    ...result.entity,
                    monitor:
                        result.entity.__typename === 'Assertion'
                            ? (result.entity.monitor?.relationships?.[0]?.entity as Maybe<Monitor>)
                            : undefined,
                    dataset:
                        result.entity.__typename === 'Assertion'
                            ? (result.entity.dataset?.relationships?.[0]?.entity as Maybe<Dataset>)
                            : undefined,
                }) as Assertion,
        ) || [];

    return (
        <Container>
            <Header>
                {/* ************************* Search Component ************************* */}
                <InlineListSearch
                    inputTestId="embedded-search-bar"
                    searchText={searchQuery}
                    debouncedSetFilterText={(value) => {
                        setSearchQuery(value);
                        analytics.event({
                            type: EventType.DatasetHealthFilterEvent,
                            tabType: 'AssertionsByAssertion',
                            filterType: 'search',
                            content: {
                                filterValue: value,
                            },
                        });
                    }}
                    matchResultCount={0}
                    numRows={0}
                    entityTypeName="assertion"
                    options={{
                        hideMatchCountText: true,
                        allowClear: true,
                    }}
                />

                {/* ************************* Filter Options ************************* */}
                <FilterOptionsWrapper>
                    {/* ************************* Status Selector ************************* */}
                    <SimpleSelect
                        width="fit-content"
                        options={DEFAULT_STATUS_OPTIONS.map((option) => ({
                            value: option,
                            label: STATUS_OPTIONS_TO_LABEL[option],
                        }))}
                        values={statuses}
                        onUpdate={(values) => {
                            if (values.length !== 0) {
                                setStatuses(values as AssertionResultTypeOptions[]);
                                analytics.event({
                                    type: EventType.DatasetHealthFilterEvent,
                                    tabType: 'AssertionsByAssertion',
                                    filterType: 'filter',
                                    filterSubType: 'assertionStatus',
                                    content: {
                                        filterValues: values,
                                    },
                                });
                            }
                        }}
                        placeholder="Results"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Results',
                        }}
                        showClear={false}
                    />
                    {/* ************************* Time Range Selector ************************* */}
                    <SimpleSelect
                        width="fit-content"
                        options={timeRangeOptions.map((option) => ({
                            value: option.label,
                            label: option.label,
                        }))}
                        values={[timeRange.label]}
                        onUpdate={(values) => {
                            setTimeRange(
                                timeRangeOptions.find((option) => option.label === values[0]) || timeRangeOptions[0],
                            );
                            analytics.event({
                                type: EventType.DatasetHealthFilterEvent,
                                tabType: 'AssertionsByAssertion',
                                filterType: 'timeRange',
                                filterSubType: timeRange.label,
                                content: {
                                    filterValues: values,
                                },
                            });
                        }}
                        placeholder="In"
                        isMultiSelect={false}
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'In',
                        }}
                        showClear={false}
                    />

                    {/* ************************* Assertion filters divider ************************* */}
                    <VerticalDivider />

                    {/* ************************* Assertion Type ************************* */}
                    <SimpleSelect
                        width="fit-content"
                        options={ASSERTION_TYPE_OPTIONS.map((option) => ({
                            value: option.value,
                            label: option.name,
                        }))}
                        values={assertionTypes}
                        onUpdate={(values) => {
                            if (values.length !== 0) {
                                setAssertionTypes(values);
                                analytics.event({
                                    type: EventType.DatasetHealthFilterEvent,
                                    tabType: 'AssertionsByAssertion',
                                    filterType: 'filter',
                                    filterSubType: 'assertionType',
                                    content: {
                                        filterValues: values,
                                    },
                                });
                            }
                        }}
                        placeholder="Type"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Type',
                        }}
                        showClear={false}
                    />

                    {/* ************************* Assertion Tags ************************* */}
                    <BaseEntityFilter
                        entityTypes={[EntityType.Tag]}
                        renderEntity={(entity) => tryGetDisplayName(entity) || entity.urn}
                        filterName="Tag"
                        fieldName={TAGS_FILTER_NAME}
                        facetState={{ facet: facets?.find((facet) => facet.field === TAGS_FILTER_NAME) }}
                        appliedFilters={{
                            filters: [
                                {
                                    field: TAGS_FILTER_NAME,
                                    values: assertionTags,
                                    condition: FilterOperator.In,
                                },
                            ],
                        }}
                        onUpdate={(values) => {
                            const selectedValues = values.filters?.[0]?.values ?? [];
                            setAssertionTags(selectedValues);
                            analytics.event({
                                type: EventType.DatasetHealthFilterEvent,
                                tabType: 'AssertionsByAssertion',
                                filterType: 'filter',
                                filterSubType: 'assertionTags',
                                content: { filterValues: selectedValues },
                            });
                        }}
                    />

                    {/* ************************* Asset filters divider ************************* */}
                    <VerticalDivider />

                    {/* ************************* Asset filters ************************* */}
                    <Tooltip
                        title="Filter by the properties of the asset that the assertion is running on."
                        placement="topLeft"
                    >
                        <div>
                            <FilterSelect
                                placeholder="Asset"
                                width="fit-content"
                                shouldDisplayConfirmationFooter
                                initialSelectedOptions={Object.entries(assetFilterOptions).flatMap(
                                    ([key, selectedOptions]) =>
                                        selectedOptions.map((value) => ({
                                            value,
                                            label: value,
                                            parentValue: key,
                                            isParent: false,
                                        })),
                                )}
                                filterOptions={{
                                    platform:
                                        facets
                                            ?.find((facet) => facet.field === ASSERTEE_PLATFORM_FILTER_NAME)
                                            ?.aggregations.map((aggregation) => ({
                                                displayName: tryGetDisplayName(aggregation.entity) || aggregation.value,
                                                category: 'platform',
                                                count: aggregation.count,
                                                name: aggregation.value,
                                            })) || [],
                                    domain:
                                        facets
                                            ?.find((facet) => facet.field === ASSERTEE_DOMAINS_FILTER_NAME)
                                            ?.aggregations.map((aggregation) => ({
                                                displayName: tryGetDisplayName(aggregation.entity) || aggregation.value,
                                                category: 'domain',
                                                count: aggregation.count,
                                                name: aggregation.value,
                                            })) || [],
                                    owner:
                                        facets
                                            ?.find((facet) => facet.field === ASSERTEE_OWNERS_FILTER_NAME)
                                            ?.aggregations.map((aggregation) => ({
                                                displayName: tryGetDisplayName(aggregation.entity) || aggregation.value,
                                                category: 'owner',
                                                count: aggregation.count,
                                                name: aggregation.value,
                                            })) || [],
                                    term:
                                        facets
                                            ?.find((facet) => facet.field === ASSERTEE_GLOSSARY_TERMS_FILTER_NAME)
                                            ?.aggregations.map((aggregation) => ({
                                                displayName: tryGetDisplayName(aggregation.entity) || aggregation.value,
                                                category: 'term',
                                                count: aggregation.count,
                                                name: aggregation.value,
                                            })) || [],
                                    tag:
                                        facets
                                            ?.find((facet) => facet.field === ASSERTEE_TAGS_FILTER_NAME)
                                            ?.aggregations.map((aggregation) => ({
                                                displayName: tryGetDisplayName(aggregation.entity) || aggregation.value,
                                                category: 'tag',
                                                count: aggregation.count,
                                                name: aggregation.value,
                                            })) || [],
                                }}
                                onFilterChange={(rawValues) => {
                                    // This is a bug in the alchemy-components library, where sometimes it returns a list of undefined values
                                    const values = rawValues.filter((value) => typeof value !== 'undefined');

                                    const domainValues = values
                                        .filter((value) => value.category === 'domain')
                                        .map((value) => value.name);
                                    const ownerValues = values
                                        .filter((value) => value.category === 'owner')
                                        .map((value) => value.name);
                                    const platformValues = values
                                        .filter((value) => value.category === 'platform')
                                        .map((value) => value.name);
                                    const termValues = values
                                        .filter((value) => value.category === 'term')
                                        .map((value) => value.name);
                                    const tagValues = values
                                        .filter((value) => value.category === 'tag')
                                        .map((value) => value.name);

                                    const newAssetFilterOptions: AssetFilterOptions = {
                                        ...assetFilterOptions,
                                    };
                                    let hasChanged = false;
                                    if (!compareListItems(domainValues, assetFilterOptions.domain)) {
                                        newAssetFilterOptions.domain = domainValues;
                                        hasChanged = true;
                                        if (domainValues.length > 0) {
                                            analytics.event({
                                                type: EventType.DatasetHealthFilterEvent,
                                                tabType: 'AssertionsByAssertion',
                                                filterType: 'filter',
                                                filterSubType: 'assetDomains',
                                                content: {
                                                    filterValues: domainValues,
                                                },
                                            });
                                        }
                                    }
                                    if (!compareListItems(ownerValues, assetFilterOptions.owner)) {
                                        newAssetFilterOptions.owner = ownerValues;
                                        hasChanged = true;
                                        if (ownerValues.length > 0) {
                                            analytics.event({
                                                type: EventType.DatasetHealthFilterEvent,
                                                tabType: 'AssertionsByAssertion',
                                                filterType: 'filter',
                                                filterSubType: 'assetOwners',
                                                content: {
                                                    filterValues: ownerValues,
                                                },
                                            });
                                        }
                                    }

                                    if (!compareListItems(platformValues, assetFilterOptions.platform)) {
                                        newAssetFilterOptions.platform = platformValues;
                                        hasChanged = true;
                                        if (platformValues.length > 0) {
                                            analytics.event({
                                                type: EventType.DatasetHealthFilterEvent,
                                                tabType: 'AssertionsByAssertion',
                                                filterType: 'filter',
                                                filterSubType: 'assetPlatforms',
                                                content: {
                                                    filterValues: platformValues,
                                                },
                                            });
                                        }
                                    }

                                    if (!compareListItems(termValues, assetFilterOptions.term)) {
                                        newAssetFilterOptions.term = termValues;
                                        hasChanged = true;
                                        if (termValues.length > 0) {
                                            analytics.event({
                                                type: EventType.DatasetHealthFilterEvent,
                                                tabType: 'AssertionsByAssertion',
                                                filterType: 'filter',
                                                filterSubType: 'assetTerms',
                                                content: {
                                                    filterValues: termValues,
                                                },
                                            });
                                        }
                                    }
                                    if (!compareListItems(tagValues, assetFilterOptions.tag)) {
                                        newAssetFilterOptions.tag = tagValues;
                                        hasChanged = true;
                                        if (tagValues.length > 0) {
                                            analytics.event({
                                                type: EventType.DatasetHealthFilterEvent,
                                                tabType: 'AssertionsByAssertion',
                                                filterType: 'filter',
                                                filterSubType: 'assetTags',
                                                content: {
                                                    filterValues: tagValues,
                                                },
                                            });
                                        }
                                    }
                                    if (hasChanged) {
                                        setAssetFilterOptions(newAssetFilterOptions);
                                    }
                                }}
                            />
                        </div>
                    </Tooltip>
                </FilterOptionsWrapper>
            </Header>
            <AssertionsByAssertionSummaryTable
                assertions={assertions}
                total={total}
                loading={loading}
                page={page}
                setPage={setPage}
                pageSize={DEFAULT_PAGE_SIZE}
                hasModifiedDefaultFilters={hasFilters}
            />
        </Container>
    );
};
