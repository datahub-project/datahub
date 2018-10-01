import Component from '@ember/component';
import { classNames } from '@ember-decorators/component';
import { computed } from '@ember-decorators/object';
import moment from 'moment';

@classNames('last-saved-by')
export default class LastSavedBy extends Component {
  /**
   * The entity responsible for the time transaction
   * @type {string}
   * @memberof LastSavedBy
   */
  actor: string;

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
  maxCharLength: number;

  /**
   * Alternate title for the component
   * @type {string}
   * @memberof LastSavedBy
   */
  title: string;

  constructor() {
    super(...arguments);

    // defaults
    (this.actor && typeof this.actor === 'string') || (this.actor = 'unknown');
    typeof this.maxCharLength === 'number' || (this.maxCharLength = 30);
    typeof this.title === 'string' || (this.title = 'Last Saved: ');
  }

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
