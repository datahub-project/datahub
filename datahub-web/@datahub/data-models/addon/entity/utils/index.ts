import { Tab, TabProperties, ITabProperties } from '@datahub/data-models/constants/entity/shared/tabs';

/**
 * Lists the TabProperties for a list of TabIds
 * @param {Array<Tab>} tabIds
 */
export const getTabPropertiesFor = (tabIds: Array<Tab>): Array<ITabProperties> =>
  tabIds.map(tabId => TabProperties[tabId]);
