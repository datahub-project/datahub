import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/user/profile/view-all';
import { layout } from '@ember-decorators/component';
import { noop } from 'lodash';
import { baseHeaderClass } from '@datahub/user/components/user/profile/entity-header';

@layout(template)
export default class UserProfileViewAll extends Component {
  /**
   * Attaching to component for convenient access in the template
   */
  baseHeaderClass: string = baseHeaderClass;

  /**
   * Title attribute passed in to be displayed in the header
   */
  title = '';

  /**
   * Tags is a list of skills or a list of teams passed in to be displayed in the content
   */
  tags: Array<string> = [];

  /**
   * External action used to trigger a parent component that the user has desired to close the
   * editor window
   */
  onCloseViewAll: () => void = noop;
}
