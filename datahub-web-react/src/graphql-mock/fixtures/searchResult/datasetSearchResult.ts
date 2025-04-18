import { generateData } from './dataGenerator';
import { datasetEntity, DatasetEntityArg } from '../entity/datasetEntity';
import { Dataset, FabricType, SearchResult, SearchResults } from '../../../types.generated';
import { EntityBrowsePath, StringNumber } from '../../types';
import { filterEntityByPath, toFlatPaths } from '../browsePathHelper';

type SearchResultArg = DatasetEntityArg;

const searchResult =
    ({ platform, origin, path }: SearchResultArg) =>
    (): SearchResult => {
        return {
            entity: datasetEntity({ platform, origin, path }),
            matchedFields: [],
            __typename: 'SearchResult',
        };
    };

export const datasetBrowsePaths: EntityBrowsePath[] = [
    {
        name: FabricType.Prod.toLowerCase(),
        paths: [
            {
                name: 'kafka',
                paths: [
                    {
                        name: 'australia',
                        paths: [
                            {
                                name: 'queensland',
                                paths: [
                                    {
                                        name: 'brisbane',
                                        paths: [
                                            {
                                                name: 'queensland-brisbane-sunnybank-vaccine-test',
                                                paths: [
                                                    {
                                                        name: 'topic-queensland-brisbane-sunnybank-vaccine-test-jan',
                                                        paths: [],
                                                        count: 2,
                                                    },
                                                    {
                                                        name: 'topic-queensland-brisbane-sunnybank-vaccine-test-feb',
                                                        paths: [],
                                                        count: 1,
                                                    },
                                                ],
                                            },
                                            {
                                                name: 'topic-queensland-brisbane-carindale-vaccine-test-jan',
                                                paths: [],
                                                count: 2,
                                            },
                                        ],
                                    },
                                    {
                                        name: 'topic-queensland-sunshine-coast-vaccine-test-feb',
                                        paths: [],
                                        count: 1,
                                    },
                                ],
                            },
                            {
                                name: 'victoria',
                                paths: [
                                    {
                                        name: 'topic-victoria-vaccine-test-mar',
                                        paths: [],
                                        count: 2,
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        name: 'topic-papua-new-guinea-digital-transformation',
                        paths: [],
                        count: 3,
                    },
                ],
            },
            {
                name: 's3',
                paths: [
                    {
                        name: 'datahub-demo-backup',
                        paths: [
                            {
                                name: 'demo',
                                paths: [],
                                count: 3,
                            },
                        ],
                    },
                    {
                        name: 'datahubproject-demo-pipelines',
                        paths: [
                            {
                                name: 'entity_aspect_splits',
                                paths: [],
                                count: 21,
                            },
                        ],
                    },
                ],
            },
            {
                name: 'snowflake',
                paths: [
                    {
                        name: 'mydb',
                        paths: [
                            {
                                name: 'schema',
                                paths: [],
                                count: 3,
                            },
                        ],
                    },
                    {
                        name: 'demo_pipeline',
                        paths: [
                            {
                                name: 'public',
                                paths: [],
                                count: 2,
                            },
                        ],
                    },
                ],
            },
            {
                name: 'bigquery',
                paths: [
                    {
                        name: 'bigquery-pubic-data',
                        paths: [
                            {
                                name: 'covid19-ecdc',
                                paths: [],
                                count: 1,
                            },
                            {
                                name: 'covid19-open-data',
                                paths: [],
                                count: 24,
                            },
                        ],
                    },
                ],
            },
        ],
    },
    {
        name: FabricType.Dev.toLowerCase(),
        paths: [
            {
                name: 'kafka',
                paths: [
                    {
                        name: 'topic-mysql-customer-schema',
                        paths: [],
                        count: 1,
                    },
                    {
                        name: 'rnd',
                        paths: [
                            {
                                name: 'topic-mysql-marketing-ml-schema',
                                paths: [],
                                count: 3,
                            },
                            {
                                name: 'topic-mysql-sales-ml-schema',
                                paths: [],
                                count: 2,
                            },
                        ],
                    },
                ],
            },
        ],
    },
];

const generateSearchResults = (): SearchResult[] => {
    return datasetBrowsePaths.flatMap(({ name: origin, paths }) => {
        return paths.flatMap(({ name: platform, paths: childPaths }) => {
            const flatPaths: StringNumber[][] = [];
            const parentPaths: string[] = [];

            toFlatPaths({ flatPaths, paths: childPaths, parentPaths });

            return flatPaths.flatMap((fp) => {
                const count = fp.pop() as number;
                const path = fp.join('.');
                return generateData<SearchResult>({
                    generator: searchResult({ platform, origin: origin.toUpperCase() as FabricType, path }),
                    count,
                });
            });
        });
    });
};

const searchResults = generateSearchResults();

export const datasetSearchResult: SearchResults = {
    start: 0,
    count: 0,
    total: 0,
    searchResults,
    facets: [
        {
            field: 'platform',
            displayName: 'platform',
            aggregations: [
                {
                    value: 's3',
                    count: 22,
                    __typename: 'AggregationMetadata',
                },
                {
                    value: 'snowflake',
                    count: 69,
                    __typename: 'AggregationMetadata',
                },
                {
                    value: 'bigquery',
                    count: 104,
                    __typename: 'AggregationMetadata',
                },
                {
                    value: 'kafka',
                    count: 7,
                    __typename: 'AggregationMetadata',
                },
            ],
            __typename: 'FacetMetadata',
        },
        {
            field: 'origin',
            displayName: 'origin',
            aggregations: [
                {
                    value: 'prod',
                    count: 202,
                    __typename: 'AggregationMetadata',
                },
            ],
            __typename: 'FacetMetadata',
        },
    ],
    __typename: 'SearchResults',
};

export const filterDatasetByPlatform = (platform: string): Dataset[] => {
    return searchResults
        .filter((r) => {
            return (r.entity as Dataset).platform.name === platform;
        })
        .map((r) => r.entity as Dataset);
};

export const filterDatasetByName = (name: string): Dataset[] => {
    return searchResults
        .filter((r) => {
            return (r.entity as Dataset).name.indexOf(name) >= 0;
        })
        .map((r) => r.entity as Dataset);
};

export const findDatasetByURN = (urn: string): Dataset => {
    return searchResults.find((r) => {
        return (r.entity as Dataset).urn === urn;
    })?.entity as Dataset;
};

export const filterDatasetByPath = (path: string[]): Dataset[] => {
    return filterEntityByPath({ term: path.slice(-2).join('.'), searchResults }) as Dataset[];
};
