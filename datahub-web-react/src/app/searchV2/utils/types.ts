/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SearchResultInterface } from '@app/entity/shared/components/styled/search/types';

import { AndFilterInput, EntityType, FacetMetadata, SearchFlags } from '@types';

/**
 * Input required to download a specific page of search results.
 */
export type DownloadSearchResultsInput = {
    scrollId: string | null | undefined;
    types?: Array<EntityType> | null;
    query: string;
    count?: number | null;
    orFilters?: Array<AndFilterInput> | null;
    viewUrn?: string | null;
    searchFlags?: SearchFlags | null;
};

/**
 * Params required to use GraphQL to fetch a specific page of search results.
 */
export type DownloadSearchResultsParams = {
    variables: {
        input: DownloadSearchResultsInput;
    };
} & Record<string, any>;

/**
 * The result of downloading a specific page of search results.
 */
export type DownloadSearchResults = {
    nextScrollId: string | undefined | null;
    count: number;
    total: number;
    searchResults: Array<SearchResultInterface>;
    facets?: Array<FacetMetadata>;
};
