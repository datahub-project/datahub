import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/user/profile/page-content';
import { layout, classNames } from '@ember-decorators/component';
import { noop } from 'lodash';

export const baseProfileContentClass = 'user-profile-content';

/**
 * The UserProfilePageContent component is used to contain the actual content under the header in
 * the user profile page, it contains the logic to determnine the tab navigation structure as well
 * as the content contained within each tab
 */
@layout(template)
@classNames(baseProfileContentClass)
export default class UserProfilePageContent extends Component {
  /**
   * Attaching to component for convenient access in the template
   */
  baseProfileContentClass: string = baseProfileContentClass;

  /**
   * The currently selected tab, identified by some sort of string key
   */
  tabSelected?: string;

  /**
   * External action expected to be passed in, should be passed along to the ivy-tabs component to
   * change the selected tab to something else. Since we have routable tabs, this action is
   * expected to be propogated to a higher container level to handle those details
   */
  changeTabSelection: (newTab: string) => void = noop;
}
