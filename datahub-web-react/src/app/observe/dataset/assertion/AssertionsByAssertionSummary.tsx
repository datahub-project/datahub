import { Select, SimpleSelect, Text, colors } from '@components';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { titleCase } from '@app/automations/utils';
import { FilterSelect } from '@app/entityV2/shared/FilterSelect';
import { InlineListSearch } from '@app/entityV2/shared/components/search/InlineListSearch';
import { AssertionsByAssertionSummaryTable } from '@app/observe/dataset/assertion/AssertionsByAssertionSummaryTable';
import {
    ASSERTION_RESULT_TYPE_OPTIONS_TO_RUN_SUMMARY_FILTER_FIELD,
    ASSERTION_TYPE_FILTER_OPTIONS,
    AssertionResultTypeOptions,
    LAST_ASSERTION_RUN_AT_SORT_FIELD,
    RUN_EVENTS_PREVIEW_LIMIT,
} from '@app/observe/dataset/assertion/constants';
import { Header } from '@app/observe/dataset/shared/shared';
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

const DEFAULT_TIME_RANGE_LABEL: TimeRange['label'] = 'Last 7 days';

const getDefaultTimeRange = (timeRangeOptions: TimeRange[]): TimeRange => {
    return timeRangeOptions.find((option) => option.label === DEFAULT_TIME_RANGE_LABEL) || timeRangeOptions[0];
};

export const AssertionsByAssertionSummary = () => {
    const entityRegistry = useEntityRegistry();

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
    const [assertionType, setAssertionType] = useState<string[]>(ASSERTION_TYPE_FILTER_OPTIONS);
    const [assertionTags, setAssertionTags] = useState<string[]>([]);
    const [assetPlatforms, setAssetPlatforms] = useState<string[]>([]);

    // Asset Filters
    const [assetOwners, setAssetOwners] = useState<string[]>([]);
    const [assetDomain, setAssetDomain] = useState<string[]>([]);
    const [assetTerms, setAssetTerms] = useState<string[]>([]);
    const [assetTags, setAssetTags] = useState<string[]>([]);

    // Reset page when filters change
    useEffect(() => {
        setPage(1);
    }, [
        searchQuery,
        statuses,
        assertionType,
        assertionTags,
        assetPlatforms,
        assetDomain,
        assetOwners,
        assetTerms,
        assetTags,
    ]);

    // Has Filters
    const hasFilters =
        statuses.length !== DEFAULT_STATUS_OPTIONS.length ||
        searchQuery.length > 0 ||
        assertionType.length > 0 ||
        assertionTags.length > 0 ||
        assetPlatforms.length > 0 ||
        assetDomain.length > 0 ||
        assetOwners.length > 0 ||
        assetTerms.length > 0 ||
        assetTags.length > 0;

    const orFilters: AndFilterInput[] = [];
    statuses.forEach((status) => {
        const filters: FacetFilterInput[] = [];
        filters.push({
            field: ASSERTION_RESULT_TYPE_OPTIONS_TO_RUN_SUMMARY_FILTER_FIELD[status],
            values: [timeRange.start.toString(), timeRange.end.toString()],
            condition: FilterOperator.Between,
        });

        // NOTE: we do not wrap these in an if statement because otherwise the facet options will not return from the backend

        filters.push({ field: ASSERTION_TYPE_FILTER_NAME, values: assertionType });

        if (assertionTags.length > 0) {
            filters.push({ field: TAGS_FILTER_NAME, values: assertionTags });
        }

        if (assetPlatforms.length > 0) {
            filters.push({ field: ASSERTEE_PLATFORM_FILTER_NAME, values: assetPlatforms });
        }

        if (assetDomain.length > 0) {
            filters.push({ field: ASSERTEE_DOMAINS_FILTER_NAME, values: assetDomain });
        }

        if (assetOwners.length > 0) {
            filters.push({ field: ASSERTEE_OWNERS_FILTER_NAME, values: assetOwners });
        }

        if (assetTerms.length > 0) {
            filters.push({ field: ASSERTEE_GLOSSARY_TERMS_FILTER_NAME, values: assetTerms });
        }

        if (assetTags.length > 0) {
            filters.push({ field: ASSERTEE_TAGS_FILTER_NAME, values: assetTags });
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
                        placeholder="Reported"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Reported',
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
                        options={ASSERTION_TYPE_FILTER_OPTIONS.map((option) => ({
                            value: option,
                            label: titleCase(option),
                        }))}
                        values={assertionType}
                        onUpdate={(values) => {
                            if (values.length !== 0) {
                                setAssertionType(values);
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
                    <Select
                        width="fit-content"
                        options={
                            facets
                                ?.find((facet) => facet.field === TAGS_FILTER_NAME)
                                ?.aggregations.map((aggregation) => ({
                                    value: aggregation.value,
                                    label: aggregation.entity
                                        ? entityRegistry.getDisplayName(aggregation.entity.type, aggregation.entity)
                                        : aggregation.value,
                                })) || []
                        }
                        values={assertionTags}
                        onUpdate={(values) => {
                            setAssertionTags(values);
                            analytics.event({
                                type: EventType.DatasetHealthFilterEvent,
                                tabType: 'AssertionsByAssertion',
                                filterType: 'filter',
                                filterSubType: 'assertionTags',
                                content: {
                                    filterValues: values,
                                },
                            });
                        }}
                        placeholder="Tags"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Tags',
                        }}
                        showClear={false}
                        emptyState={<Text color="gray">No assertion tags found.</Text>}
                    />

                    {/* ************************* Assertion Platforms ************************* */}
                    <Select
                        width="fit-content"
                        options={
                            facets
                                ?.find((facet) => facet.field === ASSERTEE_PLATFORM_FILTER_NAME)
                                ?.aggregations.map((aggregation) => ({
                                    value: aggregation.value,
                                    label: aggregation.entity
                                        ? entityRegistry.getDisplayName(aggregation.entity.type, aggregation.entity)
                                        : aggregation.value,
                                })) || []
                        }
                        values={assetPlatforms}
                        onUpdate={(values) => {
                            setAssetPlatforms(values);
                            analytics.event({
                                type: EventType.DatasetHealthFilterEvent,
                                tabType: 'AssertionsByAssertion',
                                filterType: 'filter',
                                filterSubType: 'assetPlatforms',
                                content: {
                                    filterValues: values,
                                },
                            });
                        }}
                        placeholder="Platforms"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Platforms',
                        }}
                        showClear={false}
                        emptyState={<Text color="gray">No platforms found.</Text>}
                    />

                    {/* ************************* Asset filters ************************* */}
                    <FilterSelect
                        placeholder="Asset Filters"
                        width="fit-content"
                        filterOptions={{
                            domains:
                                facets
                                    ?.find((facet) => facet.field === ASSERTEE_DOMAINS_FILTER_NAME)
                                    ?.aggregations.map((aggregation) => ({
                                        name: aggregation.value,
                                        category: 'domains',
                                        count: aggregation.count,
                                        displayName: aggregation.value,
                                    })) || [],
                            owners:
                                facets
                                    ?.find((facet) => facet.field === ASSERTEE_OWNERS_FILTER_NAME)
                                    ?.aggregations.map((aggregation) => ({
                                        name: aggregation.value,
                                        category: 'owners',
                                        count: aggregation.count,
                                        displayName: aggregation.value,
                                    })) || [],
                            terms:
                                facets
                                    ?.find((facet) => facet.field === ASSERTEE_GLOSSARY_TERMS_FILTER_NAME)
                                    ?.aggregations.map((aggregation) => ({
                                        name: aggregation.value,
                                        category: 'terms',
                                        count: aggregation.count,
                                        displayName: aggregation.value,
                                    })) || [],
                            tags:
                                facets
                                    ?.find((facet) => facet.field === ASSERTEE_TAGS_FILTER_NAME)
                                    ?.aggregations.map((aggregation) => ({
                                        name: aggregation.value,
                                        category: 'tags',
                                        count: aggregation.count,
                                        displayName: aggregation.value,
                                    })) || [],
                        }}
                        onFilterChange={(values) => {
                            const domainValues = values
                                .filter((value) => value.category === 'domains')
                                .map((value) => value.name);
                            const ownerValues = values
                                .filter((value) => value.category === 'owners')
                                .map((value) => value.name);
                            const termValues = values
                                .filter((value) => value.category === 'terms')
                                .map((value) => value.name);
                            const tagValues = values
                                .filter((value) => value.category === 'tags')
                                .map((value) => value.name);

                            setAssetDomain(domainValues);
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
                            setAssetOwners(ownerValues);
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
                            setAssetTerms(termValues);
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
                            setAssetTags(tagValues);
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
                        }}
                    />
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
