import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { Tab, ITabProperties } from '@datahub/data-models/constants/entity/shared/tabs';
import { getTabPropertiesFor } from '@datahub/data-models/entity/utils';

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
 */
export const getRenderProps = (): IEntityRenderProps => {
  const tabIds = [Tab.UserOwnership];

  return {
    entityPage: {
      tabIds,
      route: 'user.profile',
      tabProperties: getTabPropertiesFor(tabIds),
      defaultTab: Tab.UserOwnership,
      attributePlaceholder: '–'
    },
    // Placeholder information
    search: {
      attributes: [
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
        }
      ],
      searchResultEntityFields: {
        description: 'title',
        pictureUrl: 'editableInfo.pictureLink',
        name: 'info.fullName'
      },
      showFacets: false,
      placeholder: 'Search for People...',
      apiName: 'corpuser',
      autocompleteNameField: 'fullName'
    },
    // Placeholder information
    browse: {
      showCount: false,
      showHierarchySearch: false,
      entityRoute: 'user.profile'
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
