import { getExternalUrlCandidates, getExternalUrlContainTokens } from '@app/embed/lookup/utils';
import { urlEncodeUrn } from '@app/entity/shared/utils';
import { UnionType } from '@app/search/utils/constants';
import { generateOrFilters } from '@app/search/utils/generateOrFilters';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

import { GetSearchResultsForMultipleQuery, useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { FacetFilterInput, FilterOperator } from '@types';

const URL_FIELDS = ['externalUrl', 'chartUrl', 'dashboardUrl'] as const;

function buildUrlFilters(values: string[], condition: FilterOperator): FacetFilterInput[] {
    return URL_FIELDS.map((field) => ({ field, values, condition }));
}

function buildSearchInput(values: string[], condition: FilterOperator) {
    return {
        query: '*',
        start: 0,
        count: 2,
        orFilters: generateOrFilters(UnionType.OR, buildUrlFilters(values, condition)),
    };
}

function getEntities(data?: GetSearchResultsForMultipleQuery) {
    return data?.searchAcrossEntities?.searchResults?.map((result) => result.entity) ?? [];
}

const useGetEntityByUrl = (externalUrl: string) => {
    const registry = useEntityRegistry();
    const equalCandidates = getExternalUrlCandidates(externalUrl);
    const containTokens = getExternalUrlContainTokens(externalUrl);

    const { data: exactData, error: exactError } = useGetSearchResultsForMultipleQuery({
        variables: { input: buildSearchInput(equalCandidates, FilterOperator.Equal) },
    });

    const exactEntities = getEntities(exactData);

    // Substring matching is strictly a fallback: it can match sibling entities that share an
    // artifact id, so URLs that already resolve exactly must not be widened into ambiguity.
    const shouldMatchBySubstring = !!exactData && exactEntities.length === 0 && containTokens.length > 0;

    const { data: containData, error: containError } = useGetSearchResultsForMultipleQuery({
        skip: !shouldMatchBySubstring,
        variables: { input: buildSearchInput(containTokens, FilterOperator.Contain) },
    });

    const getLookupData = () => {
        if (!exactData) return {} as const;
        if (shouldMatchBySubstring && !containData) return {} as const;

        const entities = exactEntities.length ? exactEntities : getEntities(containData);
        const notFound = entities.length === 0;
        const foundMultiple = entities.length > 1;
        const entity = entities.length === 1 ? entities[0] : null;
        const embedUrl = entity
            ? `${PageRoutes.EMBED}/${registry.getPathName(entity.type)}/${urlEncodeUrn(entity.urn)}`
            : null;

        return { notFound, foundMultiple, embedUrl } as const;
    };

    return {
        error: exactError ?? containError,
        ...getLookupData(),
    } as const;
};

export default useGetEntityByUrl;
