import { ScrollDataProductsQuery } from '@graphql/dataProductsBrowse.generated';

type ScrollDataProductSearchResult = NonNullable<
    NonNullable<ScrollDataProductsQuery['scrollAcrossEntities']>['searchResults'][number]['entity']
>;
export type DataProductEntity = Extract<ScrollDataProductSearchResult, { __typename?: 'DataProduct' }>;
