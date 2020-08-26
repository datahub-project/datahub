import { ITabProperties } from '@datahub/data-models/types/entity/rendering/entity-render-props';

/**
 * Tabs available for Person entity
 */
export enum PersonTab {
  DataAccess = 'dataaccess',
  UserLists = 'userlists',
  UserOwnership = 'userownership',
  UserJitAccess = 'userjitaccess',
  UserUMPFlows = 'userumpflows',
  UserFeatureList = 'userfeaturelist',
  UserLikeList = 'userlikelist',
  UserSocialActionList = 'usersocialactionlist',
  UserDYMIList = 'userdymilist'
}

/**
 * Tab properties available for Person entity
 */
export const personTabProperties: Array<ITabProperties> = [
  {
    id: PersonTab.DataAccess,
    title: 'Data Access',
    contentComponent: '',
    tablistMenuComponent: 'user/containers/tablist/data-access'
  },
  {
    id: PersonTab.UserLists,
    title: 'Lists',
    contentComponent: '',
    tablistMenuComponent: 'user/containers/tablist/entity-lists'
  },
  {
    id: PersonTab.UserFeatureList,
    title: 'Features',
    contentComponent: 'user/containers/tab-content/entity-lists',
    lazyRender: true
  },
  {
    id: PersonTab.UserDYMIList,
    title: 'Data you might be interested in',
    contentComponent: '',
    tablistMenuComponent: 'user/containers/tablist/recommendation-list'
  },
  {
    id: PersonTab.UserOwnership,
    title: 'Ownership',
    contentComponent: '',
    tablistMenuComponent: 'user/containers/tablist/entity-ownership'
  },
  {
    id: PersonTab.UserJitAccess,
    title: 'JIT Datasets',
    contentComponent: 'user/containers/tab-content/data-access'
  },
  {
    id: PersonTab.UserUMPFlows,
    title: 'UMP Flows',
    contentComponent: 'ump-flows/route-content',
    lazyRender: true
  },
  {
    id: PersonTab.UserSocialActionList,
    title: '',
    contentComponent: 'user/containers/tab-content/social-action-list',
    lazyRender: true
  }
];

/**
 * Helper fn to filter the personTabProperties using the ids.
 * @param ids ids to filter
 */
export const getPersonTabPropertiesFor = (ids: Array<string>): Array<ITabProperties> =>
  personTabProperties.filter((tab): boolean => ids.includes(tab.id));
