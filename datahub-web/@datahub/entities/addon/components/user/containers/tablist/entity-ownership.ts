import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../../templates/components/user/containers/tablist/entity-ownership';
import { layout } from '@ember-decorators/component';
import { alias } from '@ember/object/computed';
import { ITabProperties } from '@datahub/data-models/types/entity/rendering/entity-render-props';

/**
 * This is the tablist menu component that we use to show the tablist items for the user entity
 * ownership tab. Since the tab is more complex than simple text, we use a component to specially
 * render the additional structure and functionality for the ownership segment
 */
@layout(template)
export default class UserContainersTablistEntityOwnership extends Component {
  /**
   * The list of properties that define the behavior of the tabs in this navigation tablist menu
   * component
   */
  tablistMenuProperties: Array<ITabProperties> = [];

  /**
   * From the given tablist menu, we simply redirect it to the expected property for our rendered
   * list of tab properties
   */
  @alias('tablistMenuProperties')
  displayedMenuProperties!: Array<ITabProperties>;
}
