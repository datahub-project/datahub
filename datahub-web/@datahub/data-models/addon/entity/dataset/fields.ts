import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';

/**
 * Fields for dataset
 */
export const fields: Array<ISearchEntityRenderProps> = [
  {
    showInAutoCompletion: true,
    fieldName: 'dataorigin',
    showInResultsPreview: true,
    displayName: 'Data Origin',
    showInFacets: true,
    desc: 'The data origin of the dataset',
    example: 'dataorigin:PROD',
    headerComponent: {
      name: 'dynamic-components/header',
      options: {
        className: 'search-facet__dynamic-header',
        title: 'Data Origin',
        contentComponents: [
          {
            name: 'dynamic-components/composed/user-assistance/help-tooltip-with-link',
            options: {
              text: 'The environment where the source data lives and the metadata is extracted from',
              wikiKey: 'terminologies',
              wikiLinkText: 'Learn more'
            }
          }
        ]
      }
    }
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
    example: 'owners:jweiner'
  },
  {
    showInAutoCompletion: true,
    fieldName: 'platform',
    showInResultsPreview: true,
    displayName: 'Platform',
    showInFacets: true,
    desc: 'The platform of the dataset',
    example: 'platform:kafka'
  },
  {
    showInAutoCompletion: false,
    fieldName: 'healthScore',
    showInResultsPreview: true,
    displayName: 'Health',
    showInFacets: false,
    desc: 'The health score of the dataset which is a signal for the quality of the dataset',
    example: 'N/A',
    component: {
      name: 'health/search-score'
    }
  }
];
