/* eslint-disable prefer-object-spread */
import { CorpUser, SearchResult, SearchResults } from '../../../types.generated';
import { userEntity, UserEntityArg } from '../entity/userEntity';
import { generateData } from './dataGenerator';

// login with one of these usernames
const usernames = ['kafka', 'looker', 'datahub'];

type SearchResultArg = UserEntityArg;

const searchResult = (option?: SearchResultArg) => (): SearchResult => {
    return {
        entity: userEntity(option),
        matchedFields: [],
        __typename: 'SearchResult',
    };
};

const generateSearchResults = (): SearchResult[] => {
    const loginUsers = usernames.map((username) => {
        return searchResult({ username })();
    });

    return [...loginUsers, ...generateData<SearchResult>({ generator: searchResult(), count: 25 })];
};

const searchResults = generateSearchResults();

export const userSearchResult: SearchResults = {
    start: 0,
    count: 0,
    total: 0,
    searchResults,
    facets: [],
    __typename: 'SearchResults',
};

export const findUserByUsername = (username: string): CorpUser => {
    const result = searchResults.find((r) => {
        return (r.entity as CorpUser).username === username;
    });

    return Object.assign({}, result?.entity as CorpUser);
};

export const getUsers = (): CorpUser[] => {
    return searchResults.map((r) => Object.assign({}, r.entity as CorpUser));
};

export const findUserByURN = (urn: string | null): CorpUser => {
    return searchResults.find((r) => {
        return (r.entity as CorpUser).urn === urn;
    })?.entity as CorpUser;
};
