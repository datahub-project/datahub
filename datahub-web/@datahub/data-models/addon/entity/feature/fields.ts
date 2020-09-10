import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';

/**
 * Fields for dataset
 */
export const fields: Array<ISearchEntityRenderProps> = [
  {
    showInAutoCompletion: true,
    fieldName: 'availability',
    showInResultsPreview: true,
    displayName: 'Availability',
    showInFacets: true,
    desc: 'Availability of the feature in different environments',
    example: 'offline, online and offline/online',
    minAutocompleteFetchLength: 0
  },
  {
    showInAutoCompletion: true,
    fieldName: 'baseEntity',
    showInResultsPreview: true,
    displayName: 'Base entity',
    showInFacets: true,
    desc: 'Base category of the feature',
    example: 'Member, Job',
    minAutocompleteFetchLength: 0
  },
  {
    showInAutoCompletion: true,
    fieldName: 'category',
    showInResultsPreview: true,
    displayName: 'Category',
    showInFacets: true,
    desc: 'Category of the feature',
    example: 'Job.Search',
    minAutocompleteFetchLength: 0
  },
  {
    showInAutoCompletion: true,
    fieldName: 'classification',
    showInResultsPreview: true,
    displayName: 'Classification',
    showInFacets: true,
    desc: 'Classification category of the feature',
    example: 'Characteristic, Activity',
    minAutocompleteFetchLength: 0,
    headerComponent: {
      name: 'dynamic-components/header',
      options: {
        className: 'search-facet__dynamic-header',
        title: 'Classification',
        contentComponents: [
          {
            name: 'dynamic-components/composed/user-assistance/help-tooltip-with-link',
            options: {
              text: `Whether the feature is a characteristic feature or an activity feature.
                     Characteristic features describe the Attribute of its base entity,
                     such as “Member Title”, “Job Location” etc.
                     Activity features are features generated based on member’s interactions with Linkedin`,
              wikiKey: 'terminologies',
              wikiLinkText: 'Learn more'
            }
          }
        ]
      }
    }
  },
  {
    showInAutoCompletion: false,
    fieldName: 'description',
    showInResultsPreview: false,
    displayName: 'Description',
    showInFacets: false,
    desc: '',
    example: ''
  },
  {
    showInAutoCompletion: true,
    fieldName: 'multiproduct',
    showInResultsPreview: true,
    displayName: 'Multiproduct',
    showInFacets: true,
    desc: 'URN of the frame-feature multiproduct in which this feature has been defined',
    example: 'frame-feature-hummingbird',
    minAutocompleteFetchLength: 0
  },
  {
    showInAutoCompletion: true,
    fieldName: 'name',
    showInResultsPreview: false,
    displayName: 'Name',
    showInFacets: false,
    desc: 'Name of the feature',
    example: 'member_degree'
  },
  {
    showInAutoCompletion: true,
    fieldName: 'namespace',
    showInResultsPreview: true,
    displayName: 'Namespace',
    showInFacets: false,
    desc: 'Feature namespace',
    example: 'jymbii',
    minAutocompleteFetchLength: 0
  },
  {
    showInAutoCompletion: true,
    fieldNameAlias: 'ownerEntities',
    fieldName: 'owners',
    showInResultsPreview: true,
    displayName: 'Owners',
    showInFacets: false,
    desc: 'LDAP usernames of corp users who are the owners of the multiproduct in which the feature is defined',
    example: 'nazhang',
    component: {
      name: 'avatar/generic-wrapper',
      options: { component: 'avatar/avatar-name' }
    }
  },
  {
    showInAutoCompletion: false,
    fieldName: 'sources',
    showInResultsPreview: true,
    displayName: 'Data sources',
    showInFacets: false,
    desc: '',
    example: ''
  },
  {
    showInAutoCompletion: false,
    showInFacets: true,
    showInResultsPreview: true,
    fieldName: 'tier',
    displayName: 'Tier',
    desc: 'Tier that the ML feature resides in',
    example: 'Public, Private, Deprecated, Public_Production',
    minAutocompleteFetchLength: 0,
    tagComponent: {
      name: 'search/custom-search-result-property-component/tag',
      options: {
        state: 'neutral-inverse',
        titleize: true
      }
    }
  }
];
