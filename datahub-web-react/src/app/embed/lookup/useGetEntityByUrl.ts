import { useGetSearchResultsForMultipleQuery } from '../../../graphql/search.generated';
import { FilterOperator } from '../../../types.generated';
import { UnionType } from '../../search/utils/constants';
import { generateOrFilters } from '../../search/utils/generateOrFilters';
import { PageRoutes } from '../../../conf/Global';
import { useEntityRegistry } from '../../useEntityRegistry';
import { urlEncodeUrn } from '../../entity/shared/utils';

const URL_FIELDS = ['externalUrl', 'chartUrl', 'dashboardUrl'] as const;

const useGetEntityByUrl = (externalUrl: string) => {
    const registry = useEntityRegistry();
    const { data, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                start: 0,
                count: 2,
                orFilters: generateOrFilters(
                    UnionType.OR,
                    URL_FIELDS.map((field) => ({
                        field,
                        values: [externalUrl],
                        condition: FilterOperator.Equal,
                    })),
                ),
            },
        },
    });

    const getLookupData = () => {
        if (!data) return {} as const;

        const entities = data.searchAcrossEntities?.searchResults?.map((result) => result.entity) ?? [];
        const notFound = entities.length === 0;
        const foundMultiple = entities.length > 1;
        const entity = entities.length === 1 ? entities[0] : null;
        const embedUrl = entity
            ? `${PageRoutes.EMBED}/${registry.getPathName(entity.type)}/${urlEncodeUrn(entity.urn)}`
            : null;

        return { notFound, foundMultiple, embedUrl } as const;
    };

    return {
        error,
        ...getLookupData(),
    } as const;
};

export default useGetEntityByUrl;
