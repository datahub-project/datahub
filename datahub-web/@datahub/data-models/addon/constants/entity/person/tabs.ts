import { ITabProperties, TabProperties } from '@datahub/data-models/constants/entity/shared/tabs';

/**
 * Tabs available for Person entity
 */
export enum PersonTab {
  DataAccess = 'dataaccess',
  UserLists = 'userlists',
  UserOwnership = 'userownership',
  UserJitAccess = 'userjitaccess',
  UserUMPFlows = 'userumpflows',
  UserFeatureList = 'userfeaturelist'
}

/**
 * Tab properties available for Person entity
 */
export const personTabProperties: Array<ITabProperties> = [TabProperties.userlists, TabProperties.userownership];

/**
 * Helper fn to filter the personTabProperties using the ids.
 * @param ids ids to filter
 */
export const getPersonTabPropertiesFor = (ids: Array<string>): Array<ITabProperties> =>
  personTabProperties.filter((tab): boolean => ids.includes(tab.id));
