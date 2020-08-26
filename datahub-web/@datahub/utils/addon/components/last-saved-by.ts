import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/last-saved-by';
import { classNames, layout } from '@ember-decorators/component';
import { computed } from '@ember/object';
import moment from 'moment';

@classNames('last-saved-by')
@layout(template)
export default class LastSavedBy extends Component {
  /**
   * The entity responsible for the time transaction
   * @type {string}
   * @memberof LastSavedBy
   */
  actor = 'unknown';

  /**
   * The time at which the related transaction occurred
   * @type {(number | undefined)}
   * @memberof LastSavedBy
   */
  time: number | undefined;

  /**
   * Maximum length of characters to display of the actor
   * @type {number}
   * @memberof LastSavedBy
   */
  maxCharLength = 30;

  /**
   * Alternate title for the component
   * @type {string}
   * @memberof LastSavedBy
   */
  title = 'Last Saved:';

  /**
   * Resolves the appropriate representation for the time the transaction occurred
   * @readonly
   * @type {string}
   * @memberof LastSavedBy
   */
  @computed('time')
  get lastSaved(): string {
    const { time } = this;

    return time ? moment(time).fromNow() : 'Never';
  }
}
