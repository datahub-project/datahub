import { ASSERTION_SOURCES_OPTIONS, AssertionResultTypeOptions } from '@app/observe/dataset/assertion/constants';
import { QueryParamDecoder, QueryParamEncoder } from '@app/observe/dataset/shared/util';

export const DEFAULT_PAGE_SIZE = 25;

const STATUS_OPTIONS: AssertionResultTypeOptions[] = ['Failing', 'Error', 'Passing'];
export const DEFAULT_STATUS_OPTIONS: AssertionResultTypeOptions[] = ['Failing', 'Error', 'Passing'];

export const STATUS_OPTIONS_TO_LABEL: Record<AssertionResultTypeOptions, string> = {
    Failing: 'At least one failure',
    Error: 'At least one error',
    Passing: 'At least one success',
};

// Time Range
export type TimeRange = {
    start: number;
    end: number;
    label: 'Last 24 hours' | 'Last 7 days' | 'Last 30 days' | 'Last 1 year';
    urlParam: 'l_24_h' | 'l_7_d' | 'l_30_d' | 'l_1_y';
};

export const TIME_RANGE_OPTIONS = ((): TimeRange[] => {
    const now = new Date().getTime();
    const oneDayAgo = new Date(now - 1 * 24 * 60 * 60 * 1000);
    const oneWeekAgo = new Date(now - 7 * 24 * 60 * 60 * 1000);
    const oneMonthAgo = new Date(now - 30 * 24 * 60 * 60 * 1000);
    const oneYearAgo = new Date(now - 365 * 24 * 60 * 60 * 1000);
    return [
        { start: oneDayAgo.getTime(), end: now, label: 'Last 24 hours', urlParam: 'l_24_h' },
        { start: oneWeekAgo.getTime(), end: now, label: 'Last 7 days', urlParam: 'l_7_d' },
        { start: oneMonthAgo.getTime(), end: now, label: 'Last 30 days', urlParam: 'l_30_d' },
        { start: oneYearAgo.getTime(), end: now, label: 'Last 1 year', urlParam: 'l_1_y' },
    ];
})();

export const DEFAULT_TIME_RANGE_LABEL: TimeRange['label'] = 'Last 7 days';
export const DEFAULT_TIME_RANGE =
    TIME_RANGE_OPTIONS.find((option) => option.label === DEFAULT_TIME_RANGE_LABEL) || TIME_RANGE_OPTIONS[0];

// Filter Options
export type AssetFilterOptions = {
    platform: string[];
    container: string[];
    domain: string[];
    owner: string[];
    term: string[];
    tag: string[];
};

export type FitlerOptions = {
    query: string;
    page: number;
    size: number;
    statuses: AssertionResultTypeOptions[];
    timeRange: TimeRange;
    // assertion filters
    types: string[];
    source: string;
    tags: string[];
    // asset filters
    asset_platform: string[];
    asset_container: string[];
    asset_domain: string[];
    asset_owner: string[];
    asset_term: string[];
    asset_tag: string[];
};

export const DEFAULT_FILTER_OPTIONS: FitlerOptions = {
    // pagination
    page: 1,
    size: DEFAULT_PAGE_SIZE,

    // search
    query: '',

    // filters
    statuses: DEFAULT_STATUS_OPTIONS,
    timeRange: DEFAULT_TIME_RANGE,

    // assertion filters
    types: [],
    source:
        ASSERTION_SOURCES_OPTIONS.find((option) => option.value === 'All')?.value || ASSERTION_SOURCES_OPTIONS[0].value,
    tags: [],

    // asset filters
    asset_platform: [],
    asset_container: [],
    asset_domain: [],
    asset_owner: [],
    asset_term: [],
    asset_tag: [],
};

// Decodes url query params to filter options
export const FILTER_OPTIONS_DECODER: QueryParamDecoder<FitlerOptions> = {
    page: (value: string) => parseInt(value, 10),
    size: (value: string) => parseInt(value, 10),
    query: (value: string) => decodeURIComponent(value),
    statuses: (value: string) =>
        value
            .split(',')
            .map((element) => decodeURIComponent(element) as AssertionResultTypeOptions)
            .filter((el) => STATUS_OPTIONS.includes(el as AssertionResultTypeOptions)),
    timeRange: (value: string) => TIME_RANGE_OPTIONS.find((option) => option.urlParam === decodeURIComponent(value)),
    types: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
    source: (value: string) => decodeURIComponent(value),
    tags: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
    asset_platform: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
    asset_container: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
    asset_domain: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
    asset_owner: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
    asset_term: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
    asset_tag: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
};

// Encodes filter options to url query params
export const FILTER_OPTIONS_ENCODER: QueryParamEncoder<FitlerOptions> = {
    page: (value: number) => value.toString(),
    size: (value: number) => value.toString(),
    query: (value: string) => encodeURIComponent(value),
    statuses: (value: AssertionResultTypeOptions[]) => value.map((element) => encodeURIComponent(element)).join(','),
    timeRange: (value: TimeRange) => encodeURIComponent(value.urlParam),
    types: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    source: (value: string) => encodeURIComponent(value),
    tags: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    asset_platform: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    asset_container: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    asset_domain: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    asset_owner: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    asset_term: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    asset_tag: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
};
