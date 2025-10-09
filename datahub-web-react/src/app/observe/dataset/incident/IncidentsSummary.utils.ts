import { QueryParamDecoder, QueryParamEncoder } from '@app/observe/dataset/shared/util';

export const DEFAULT_PAGE_SIZE = 10;

export const INCIDENTS_DOCS_LINK = 'https://docs.datahub.com/docs/incidents/incidents';

export type FilterOptions = {
    // pagination
    page: number;
    size: number;

    // search
    query: string;

    // filters
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
    domains: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    owners: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    platforms: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    containers: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    terms: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
    tags: (value: string[]) => value.map((element) => encodeURIComponent(element)).join(','),
};
