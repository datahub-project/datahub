import { AssertionResultTypeOptions } from '@app/observe/dataset/assertion/constants';
import { QueryParamDecoder, QueryParamEncoder } from '@app/observe/dataset/shared/util';

export const DEFAULT_PAGE_SIZE = 25;

export const DEFAULT_STATUS_OPTIONS: AssertionResultTypeOptions[] = ['Failing', 'Passing', 'Error'];

export type FilterOptions = {
    page: number;
    size: number;
    query: string;
    statuses: AssertionResultTypeOptions[];
    domains: string[];
    owners: string[];
    platforms: string[];
    containers: string[];
    terms: string[];
    tags: string[];
};

export const DEFAULT_FILTER_OPTIONS: FilterOptions = {
    page: 1,
    size: DEFAULT_PAGE_SIZE,
    query: '',
    statuses: DEFAULT_STATUS_OPTIONS,
    domains: [],
    owners: [],
    platforms: [],
    containers: [],
    terms: [],
    tags: [],
};

export const FILTER_OPTIONS_DECODER: QueryParamDecoder<FilterOptions> = {
    page: (value: string) => parseInt(value, 10),
    size: (value: string) => parseInt(value, 10),
    query: (value: string) => decodeURIComponent(value),
    statuses: (value: string) =>
        value.split(',').map((element) => decodeURIComponent(element) as AssertionResultTypeOptions),
    domains: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
    owners: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
    platforms: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
    containers: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
    terms: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
    tags: (value: string) => value.split(',').map((element) => decodeURIComponent(element)),
};

export const FILTER_OPTIONS_ENCODER: QueryParamEncoder<FilterOptions> = {
    page: (value: number) => value.toString(),
    size: (value: number) => value.toString(),
    query: (value: string) => encodeURIComponent(value),
    statuses: (value: AssertionResultTypeOptions[]) => value.map((element) => encodeURIComponent(element)).join(','),
    domains: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    owners: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    platforms: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    containers: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    terms: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    tags: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
};
