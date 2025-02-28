import { useUserContext } from '@src/app/context/useUserContext';
import { useGetDataProductsListQuery } from '@src/graphql/dataProduct.generated';
import { DataProduct, Domain, EntityType } from '../../../../../../../types.generated';

const FETCH_COUNT = 100;
const sortDataProducts = (a, b) => {
    return b.dataProduct?.properties?.numAssets - a.dataProduct?.properties?.numAssets;
};

export const useGetDataProducts = (): {
    dataProducts: { dataProduct: DataProduct | any; domain: Domain }[];
    loading: boolean;
} => {
    const { localState } = useUserContext();
    const { selectedViewUrn } = localState;

    const { data, loading } = useGetDataProductsListQuery({
        variables: {
            input: {
                types: [EntityType.DataProduct],
                query: '*',
                start: 0,
                count: FETCH_COUNT,
                viewUrn: selectedViewUrn,
            },
        },
        fetchPolicy: 'cache-first',
    });

    const results = data?.searchAcrossEntities?.searchResults || [];

    const dataProducts = results
        .map((item): { dataProduct: DataProduct; domain: Domain } => {
            const dataProduct = item.entity as any;
            const { domain } = dataProduct;
            return {
                dataProduct,
                domain: domain ? domain.domain : null,
            };
        })
        .filter((item) => item.domain)
        // Sorting based on popularity
        .sort(sortDataProducts);

    return { dataProducts, loading };
};
