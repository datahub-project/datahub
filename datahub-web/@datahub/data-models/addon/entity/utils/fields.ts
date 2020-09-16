import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';

/**
 * Default sorting fn for ISearchEntityRenderProps
 */
export const sortFields = (fieldA: ISearchEntityRenderProps, fieldB: ISearchEntityRenderProps): number =>
  fieldA.displayName.toLowerCase() < fieldB.displayName.toLowerCase() ? -1 : 1;
