import { DataFlow, SearchResult, SearchResults } from '../../../types.generated';
import { EntityBrowsePath } from '../../types';
import { filterEntityByPath } from '../browsePathHelper';
import { dataFlowEntity, DataFlowEntityArg } from '../entity/dataFlowEntity';
import { generateData } from './dataGenerator';

type SearchResultArg = DataFlowEntityArg;

const searchResult =
    ({ orchestrator, cluster }: SearchResultArg) =>
    (): SearchResult => {
        return {
            entity: dataFlowEntity({ orchestrator, cluster }),
            matchedFields: [],
            __typename: 'SearchResult',
        };
    };

export const dataFlowBrowsePaths: EntityBrowsePath[] = [
    {
        name: 'airflow',
        paths: [
            {
                name: 'prod',
                paths: [],
                count: 4,
            },
            {
                name: 'dev',
                paths: [],
                count: 1,
            },
        ],
    },
];

const generateSearchResults = (): SearchResult[] => {
    return dataFlowBrowsePaths.flatMap(({ name: orchestrator, paths }) => {
        return paths.flatMap(({ name: cluster, count = 0 }) => {
            return generateData<SearchResult>({ generator: searchResult({ orchestrator, cluster }), count });
        });
    });
};

const searchResults = generateSearchResults();

export const dataFlowSearchResult: SearchResults = {
    start: 0,
    count: 0,
    total: 0,
    searchResults,
    facets: [],
    __typename: 'SearchResults',
};

export const filterDataFlowByOrchestrator = (orchestrator: string): DataFlow[] => {
    return searchResults
        .filter((r) => {
            return (r.entity as DataFlow).orchestrator === orchestrator;
        })
        .map((r) => r.entity as DataFlow);
};

export const findDataFlowByURN = (urn: string): DataFlow => {
    return searchResults.find((r) => {
        return (r.entity as DataFlow).urn === urn;
    })?.entity as DataFlow;
};

export const filterDataFlowByPath = (path: string[]): DataFlow[] => {
    return filterEntityByPath({ term: path.slice(-2).join(',[\\s\\S]+,'), searchResults }) as DataFlow[];
};
