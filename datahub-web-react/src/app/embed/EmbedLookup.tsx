import React, { useEffect, useMemo } from 'react';
import { useHistory, useParams } from 'react-router';
import { PageRoutes } from '../../conf/Global';
import { FilterOperator } from '../../types.generated';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { generateOrFilters } from '../search/utils/generateOrFilters';
import { UnionType } from '../search/utils/constants';
import { useEntityRegistry } from '../useEntityRegistry';
import { urlEncodeUrn } from '../entity/shared/utils';

interface RouteParams {
    url: string;
}

const EmbedLookup = () => {
    const history = useHistory();
    const registry = useEntityRegistry();
    const { url: encodedUrl } = useParams<RouteParams>();
    const decodedUrl = decodeURIComponent(encodedUrl);

    const { data, loading, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                start: 0,
                count: 2,
                orFilters: generateOrFilters(
                    UnionType.OR,
                    ['externalUrl', 'chartUrl', 'dashboardUrl'].map((field) => ({
                        field,
                        values: [decodedUrl],
                        condition: FilterOperator.Equal,
                    })),
                ),
            },
        },
    });

    const results = data?.searchAcrossEntities?.searchResults;
    const notFound = !!results && results.length === 0;
    const multipleResults = !!results && results.length > 1;

    const destinationUrl = useMemo(() => {
        if (!results) return null;
        if (notFound) return null;
        if (multipleResults) return null;
        const firstReesult = results[0];
        const { entity } = firstReesult;
        if (!entity) return null;
        return [PageRoutes.EMBED, registry.getPathName(entity.type), urlEncodeUrn(entity.urn)].join('/');
    }, [multipleResults, notFound, registry, results]);

    useEffect(() => {
        if (destinationUrl) {
            console.log('routing to', destinationUrl, 'from', history.location);
        }
    }, [destinationUrl, history.location]);

    if (loading) return <div>Loading...</div>;
    if (error) return <div>{JSON.stringify(error)}</div>;
    if (notFound) return <div>Not found</div>;
    if (multipleResults) return <div>Multiple results</div>;

    return <div>Redirect to {destinationUrl}</div>;
};

export default EmbedLookup;
