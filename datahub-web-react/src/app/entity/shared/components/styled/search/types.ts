import { UnionType } from '@app/search/utils/constants';

import {
    Entity,
    FacetFilterInput,
    FacetMetadata,
    LineageSearchPath,
    MatchedField,
    Maybe,
    Scalars,
    SearchAcrossEntitiesInput,
    SearchInsight,
} from '@types';

export type GetSearchResultsParams = {
    variables: {
        input: SearchAcrossEntitiesInput;
    };
} & Record<string, any>;

export type SearchResultInterface = {
    entity: Entity;
    /** Insights about why the search result was matched */
    insights?: Maybe<Array<SearchInsight>>;
    /** Matched field hint */
    matchedFields: Array<MatchedField>;
    degree?: Maybe<number>;
} & Record<string, any>;

export type SearchResultsInterface = {
    /** The offset of the result set */
    start: Scalars['Int'];
    /** The number of entities included in the result set */
    count: Scalars['Int'];
    /** The total number of search results matching the query and filters */
    total: Scalars['Int'];
    /** The search result entities */
    searchResults: Array<SearchResultInterface>;
    /** Candidate facet aggregations used for search filtering */
    facets?: Maybe<Array<FacetMetadata>>;
    /** The path taken when doing search across lineage */
    lineageSearchPath?: Maybe<LineageSearchPath>;
};

/**
 * Supported Action Groups for search-select feature.
 */
export enum SelectActionGroups {
    CHANGE_OWNERS,
    CHANGE_TAGS,
    CHANGE_GLOSSARY_TERMS,
    CHANGE_DOMAINS,
    CHANGE_DEPRECATION,
    DELETE,
    CHANGE_DATA_PRODUCTS,
}

/**
 * A fixed set of Filters, joined in conjunction or disjunction.
 */
export type FilterSet = {
    unionType: UnionType;
    filters: FacetFilterInput[];
};
