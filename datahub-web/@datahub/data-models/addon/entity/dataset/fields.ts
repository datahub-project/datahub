import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';

/**
 * Fields for dataset
 */
export const fields: Array<ISearchEntityRenderProps> = [
  {
    showInAutoCompletion: true,
    fieldName: 'origin',
    showInResultsPreview: true,
    displayName: 'Origin',
    showInFacets: true,
    desc: 'The origin of the dataset',
    example: 'origin:PROD'
  },
  {
    showInAutoCompletion: true,
    fieldName: 'name',
    showInResultsPreview: false,
    displayName: 'name',
    showInFacets: false,
    desc: 'The name of the dataset',
    example: 'name:TRACKING.PageViewEvent'
  },
  {
    showInAutoCompletion: true,
    fieldName: 'owners',
    showInResultsPreview: true,
    displayName: 'owners',
    showInFacets: false,
    desc: 'The confirmed owners for the dataset',
    example: 'owners:sweaver'
  },
  {
    showInAutoCompletion: true,
    fieldName: 'platform',
    showInResultsPreview: true,
    displayName: 'Platform',
    showInFacets: true,
    desc: 'The platform of the dataset',
    example: 'platform:kafka'
  }
];
