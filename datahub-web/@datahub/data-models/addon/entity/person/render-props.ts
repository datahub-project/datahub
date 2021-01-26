import {
  IEntityRenderProps,
  IEntityRenderPropsEntityPage,
  ITabProperties
} from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { PersonTab, getPersonTabPropertiesFor } from '@datahub/data-models/constants/entity/person/tabs';

/**
 * Specific render properties only to the person entity
 */
export interface IPersonEntitySpecificConfigs {
  userProfilePage: {
    headerProperties: {
      showExternalProfileLink?: boolean;
      externalProfileLinkText?: string;
      isConnectedToLinkedin?: boolean;
      isConnectedToSlack?: boolean;
    };
    tablistMenuProperties: Record<string, Array<ITabProperties>>;
  };
}

/**
 * Class properties common across instances
 * Dictates how visual ui components should be rendered
 * Implemented as a getter to ensure that reads are idempotent
 *
 * Making sure entityPage is not marked optional in the types as we know it is defined
 */
export const getRenderProps = (): IEntityRenderProps & {
  entityPage: IEntityRenderPropsEntityPage;
} => {
  const tabIds = [PersonTab.UserOwnership];
  const aspects: Array<keyof Com.Linkedin.Metadata.Aspect.CorpUserAspect> = ['com.linkedin.identity.CorpUserInfo'];

  return {
    apiEntityName: 'corpuser',
    entityPage: {
      apiRouteName: 'corpusers',
      route: 'user.profile',
      tabProperties: getPersonTabPropertiesFor(tabIds),
      defaultTab: PersonTab.UserOwnership,
      attributePlaceholder: '–'
    },
    // Placeholder information
    search: {
      attributes: [
        {
          fieldName: 'reportsTo.entityLink.link',
          component: {
            name: 'link/optional-value'
          },
          showInResultsPreview: true,
          showInAutoCompletion: false,
          showInFacets: false,
          displayName: 'Manager',
          desc: '',
          example: ''
        },
        {
          fieldName: 'teamTags',
          showInResultsPreview: true,
          showInAutoCompletion: false,
          showInFacets: false,
          displayName: 'Team',
          desc: '',
          example: ''
        },
        {
          fieldName: 'skills',
          showInResultsPreview: true,
          showInAutoCompletion: false,
          showInFacets: false,
          displayName: 'Ask me about',
          desc: '',
          example: ''
        },
        {
          showInAutoCompletion: false,
          fieldName: 'inactive',
          showInResultsPreview: false,
          displayName: 'Inactive',
          showInFacets: false,
          desc: '',
          example: '',
          tagComponent: {
            name: 'search/custom-search-result-property-component/tag',
            options: {
              state: 'alert',
              text: 'Inactive'
            }
          }
        }
      ],
      searchResultEntityFields: {
        description: 'title',
        pictureUrl: 'profilePictureUrl'
      },
      showFacets: false,
      placeholder: 'Search for People...',
      autocompleteNameField: 'fullName',
      isEnabled: true,
      defaultAspects: aspects
    }
  };
};

/**
 * Properties for render props that are only applicable to the person entity. Dictates how UI
 * components should be rendered for this entity
 */
export const getPersonEntitySpecificRenderProps = (): IPersonEntitySpecificConfigs => ({
  userProfilePage: {
    headerProperties: {
      showExternalProfileLink: false
    },
    tablistMenuProperties: {}
  }
});
