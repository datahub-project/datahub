import { attributeDisplayName } from '@datahub/data-models/entity/feature/utils';
import { ArrayElement } from '@datahub/utils/types/array';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';

// Lists properties for rendering a Feature in a list
type ListItemFields = Array<ISearchEntityRenderProps>;

/**
 * Specifies the attributes to be displayed per feature in an EntityListContainer
 */
export const entityListRenderFields: ListItemFields = [
  {
    displayName: attributeDisplayName.baseEntity,
    fieldName: 'baseEntity'
  },
  {
    displayName: attributeDisplayName.classification,
    fieldName: 'classification'
  },
  {
    displayName: attributeDisplayName.category,
    fieldName: 'categoryPathString'
  },
  {
    displayName: attributeDisplayName.frameMp,
    fieldName: 'namespace'
  },
  {
    displayName: attributeDisplayName.availability,
    fieldName: 'formattedDataAvailability'
  }
].map(
  (field): ArrayElement<NonNullable<IEntityRenderProps['list']>['searchResultConfig']['attributes']> => ({
    ...field,
    showInResultsPreview: true,
    showInAutoCompletion: false,
    showInFacets: false,
    desc: '',
    example: ''
  })
);
