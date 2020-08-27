import { ITabProperties } from '@datahub/data-models/types/entity/rendering/entity-render-props';

/**
 * Lists the tabs shared globally
 * @export
 * @enum {string}
 */
export enum CommonTab {
  Wiki = 'wiki'
}

/**
 * List the tab properties of shared tabs
 */
export const CommonTabProperties: Array<ITabProperties> = [
  {
    id: CommonTab.Wiki,
    title: 'Docs',
    contentComponent: 'institutional-memory/containers/tab',
    lazyRender: true
  }
];
