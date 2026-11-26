import { ScrollDataProductsQuery } from '@graphql/marketplaceBrowse.generated';

type ScrollDataProductSearchResult = NonNullable<
    NonNullable<ScrollDataProductsQuery['scrollAcrossEntities']>['searchResults'][number]['entity']
>;
export type DataProductEntity = Extract<ScrollDataProductSearchResult, { __typename?: 'DataProduct' }>;
