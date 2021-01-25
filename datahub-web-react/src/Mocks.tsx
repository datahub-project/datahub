import { GetDatasetDocument } from './graphql/dataset.generated';
import { GetBrowsePathsDocument, GetBrowseResultsDocument } from './graphql/browse.generated';
import { GetAutoCompleteResultsDocument, GetSearchResultsDocument } from './graphql/search.generated';
import { LoginDocument } from './graphql/auth.generated';

const user1 = {
    username: 'sdas',
    urn: 'urn:li:corpuser:2',
    info: {
        active: true,
        displayName: 'sdas',
        title: 'Software Engineer',
        firstName: 'Shirshanka',
        lastName: 'Das',
        fullName: 'Shirshanka Das',
    },
    editableInfo: {
        pictureLink: 'https://crunchconf.com/img/2019/speakers/1559291783-ShirshankaDas.png',
    },
};

const user2 = {
    username: 'john',
    urn: 'urn:li:corpuser:3',
    info: {
        active: true,
        displayName: 'john',
        title: 'Eng',
        firstName: 'John',
        lastName: 'Joyce',
        fullName: 'John Joyce',
    },
    editableInfo: {
        pictureLink: null,
    },
};

const dataset1 = {
    urn: 'urn:li:dataset:1',
    platform: 'HDFS',
    platformNativeType: 'TABLE',
    name: 'The Great Test Dataset',
    origin: 'PROD',
    tags: ['Private', 'PII'],
    description: 'This is the greatest dataset in the world, youre gonna love it!',
    uri: 'www.google.com',
    properties: [],
    createdTime: 0,
    modifiedTime: 0,
    ownership: {
        owners: [
            {
                owner: {
                    ...user1,
                },
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                type: 'DELEGATE',
            },
        ],
        lastModified: 0,
    },
};

const dataset2 = {
    urn: 'urn:li:dataset:2',
    platform: 'MySQL',
    platformNativeType: 'TABLE',
    name: 'Some Other Dataset',
    origin: 'PROD',
    tags: ['Outdated'],
    description: 'This is some other dataset, so who cares!',
    uri: 'www.google.com',
    properties: [],
    createdTime: 0,
    modifiedTime: 0,
    ownership: {
        owners: [
            {
                owner: {
                    ...user1,
                },
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                type: 'DELEGATE',
            },
        ],
        lastModified: 0,
    },
};

const dataset3 = {
    urn: 'urn:li:dataset:3',
    platform: 'Kafka',
    platformNativeType: 'STREAM',
    name: 'Yet Another Dataset',
    origin: 'PROD',
    tags: ['Trusted'],
    description: 'This and here we have yet another Dataset (YAN). Are there more?',
    uri: 'www.google.com',
    properties: [],
    createdTime: 0,
    modifiedTime: 0,
    ownership: {
        owners: [
            {
                owner: {
                    ...user1,
                },
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                type: 'DELEGATE',
            },
        ],
        lastModified: 0,
    },
};

/*
    Define mock data to be returned by Apollo MockProvider. 
*/
export const mocks = [
    {
        request: {
            query: LoginDocument,
            variables: {
                username: 'datahub',
                password: 'datahub',
            },
        },
        result: {
            data: {
                login: {
                    ...user1,
                },
            },
        },
    },
    {
        request: {
            query: GetDatasetDocument,
            variables: {
                urn: 'urn:li:dataset:1',
            },
        },
        result: {
            data: {
                dataset: {
                    ...dataset1,
                },
            },
        },
    },
    {
        request: {
            query: GetBrowsePathsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    urn: 'urn:li:dataset:1',
                },
            },
        },
        result: {
            data: {
                browsePaths: [['prod', 'hdfs', 'datasets']],
            },
        },
    },
    {
        request: {
            query: GetBrowseResultsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    path: ['prod', 'hdfs'],
                    start: 0,
                    count: 10,
                },
            },
        },
        result: {
            data: {
                browse: {
                    entities: [
                        {
                            urn: 'urn:li:dataset:1',
                            name: 'Test Dataset',
                        },
                    ],
                    start: 0,
                    count: 1,
                    total: 1,
                    metadata: {
                        path: ['prod', 'hdfs'],
                        groups: [],
                        totalNumEntities: 0,
                    },
                },
            },
        },
    },
    {
        request: {
            query: GetAutoCompleteResultsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    query: 't',
                    field: 'name',
                },
            },
        },
        result: {
            data: {
                autoComplete: {
                    query: 't',
                    suggestions: ['The Great Test Dataset', 'Some other test'],
                },
            },
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [],
                },
            },
        },
        result: {
            data: {
                search: {
                    start: 0,
                    count: 3,
                    total: 3,
                    elements: [
                        {
                            __typename: 'Dataset',
                            ...dataset1,
                        },
                        {
                            __typename: 'Dataset',
                            ...dataset2,
                        },
                        {
                            __typename: 'Dataset',
                            ...dataset3,
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            aggregations: [{ value: 'PROD', count: 3 }],
                        },
                        {
                            field: 'platform',
                            aggregations: [
                                { value: 'HDFS', count: 1 },
                                { value: 'MySQL', count: 1 },
                                { value: 'Kafka', count: 1 },
                            ],
                        },
                    ],
                },
            },
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [
                        {
                            field: 'platform',
                            value: 'Kafka',
                        },
                    ],
                },
            },
        },
        result: {
            data: {
                search: {
                    start: 0,
                    count: 1,
                    total: 1,
                    elements: [
                        {
                            __typename: 'Dataset',
                            ...dataset3,
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            aggregations: [{ value: 'PROD', count: 3 }],
                        },
                        {
                            field: 'platform',
                            aggregations: [
                                { value: 'HDFS', count: 1 },
                                { value: 'MySQL', count: 1 },
                                { value: 'Kafka', count: 1 },
                            ],
                        },
                    ],
                },
            },
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    type: 'USER',
                    query: 'Test',
                    start: 0,
                    count: 10,
                },
            },
        },
        result: {
            data: {
                search: {
                    start: 0,
                    count: 2,
                    total: 2,
                    elements: [
                        {
                            ...user1,
                        },
                    ],
                },
            },
        },
    },
];
