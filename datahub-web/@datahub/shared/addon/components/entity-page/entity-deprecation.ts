import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../../templates/components/entity-page/entity-deprecation';
import { action, computed } from '@ember/object';
import { reads } from '@ember/object/computed';
import { classNames } from '@ember-decorators/component';
import moment from 'moment';
import { set } from '@ember/object';

/**
 * This component was made as a tab to handle the deprecation scenario for entities on DataHub, but is made generic to
 * be usable or extendable for a general deprecation scenarios
 *
 * @example
 * {{datahub/entity-page/entity-deprecation
 *   entityName="Pokemon"
 *   deprecated=boolDeprecated
 *   deprecationNote=stringDeprecationNote
 *   decommissionTime=timestampDecommissionTime
 *   entityDecommissionWikiLink=hashWikiLinks.entityDecommission
 *   onUpdateDeprecation=functionOrActionUpdateDeprecation}}
 */
@classNames('entity-deprecation')
export default class EntityDeprecation extends Component {
  layout = layout;

  /**
   * Name of the current entity that we are dealing with. Originally this was "dataset" but is
   * now expanding
   * @type {string}
   */
  entityName!: string;

  /**
   * Flag indicating whether the current entity has been deprecated
   * @type {boolean}
   */
  isDeprecated!: boolean;

  /**
   * Text string, intended to indicate the reason for deprecation
   * @type {string}
   */
  deprecationNote!: string;

  /**
   * Time when the dataset will be decommissioned
   * @type {number}
   */
  decommissionTime!: number;

  /**
   * Expected to be passed in from a containing component, will be called to actually update the
   * component with the new user input
   * @param {boolean} isDeprecated - new deprecation state
   * @param {string} note - new deprecation note
   * @param {number} time - new deprecation time as unix timestamp
   */
  updateDeprecation!: (isDeprecated: boolean, note: string, time: number | null) => Promise<void>;

  /**
   * Currently selected date, used in the calendar dropdown component
   * @type {Date}
   */
  selectedDate: Date = new Date();

  /**
   * Date around which the calendar is centered
   * @type {Date}
   */
  centeredDate: Date = this.selectedDate;

  /**
   * Before a user can update the deprecation status to deprecated, they must acknowledge that even if the
   * entity is deprecated they must still keep it compliant.
   * @type {boolean}
   */
  isDeprecationAcknowledged = false;

  /**
   * Expected to be passed in if we plan on using the default entity deprecation acknowledgement template,
   * leads to a more info link for the user about deprecation of such entity
   * @type {string | undefined}
   */
  entityDecommissionWikiLink?: string;

  /**
   * Optionally passed in if the entity should have an acknowledgement message/structure that differs from
   * our default provided partial. If not passed in, constructor will automatically populate this with the
   * default acknowledgement
   * @type {string}
   */
  deprecationAcknowledgementTemplate!: string;

  /**
   * The earliest date a user can select as a decommission date
   * @type {Date}
   */
  minSelectableDecommissionDate: Date = new Date(
    moment()
      .add(1, 'days')
      .valueOf()
  );

  /**
   * Working reference to the entity's deprecated flag, made to be edited by the user
   * @type {ComputedProperty<boolean>}
   */
  @reads('isDeprecated')
  isDeprecatedAlias!: EntityDeprecation['isDeprecated'];

  /**
   * Working reference to the entity's decommissionTime flag, made to be edited by the user
   */
  @reads('decommissionTime')
  decommissionTimeAlias!: EntityDeprecation['decommissionTime'];

  /**
   * Working reference to the entity's deprecationNote, made to be edited by the user
   * @type {ComputedProperty<EntityDeprecation.deprecationNote>}
   */
  @computed('deprecationNote')
  get deprecationNoteAlias(): EntityDeprecation['deprecationNote'] {
    return this.deprecationNote || '';
  }

  didInsertElement(): void {
    super.didInsertElement();

    typeof this.deprecationAcknowledgementTemplate === 'string' ||
      set(this, 'deprecationAcknowledgementTemplate', 'partials/entity-deprecation/default-acknowledgement');
  }

  /**
   * Invokes the save action with the updated values for deprecated status, decommission time, and
   * deprecation note. The actual save functionality should be handled by
   */
  @action
  async onUpdateDeprecation(): Promise<void> {
    const { isDeprecatedAlias, deprecationNoteAlias, decommissionTimeAlias } = this;

    const noteValue = isDeprecatedAlias ? deprecationNoteAlias : '';
    const time = decommissionTimeAlias || null;

    await this.updateDeprecation(!!isDeprecatedAlias, noteValue || '', time);
    set(this, 'deprecationNoteAlias', noteValue);
  }

  /**
   * Toggle the boolean value of our deprecated alias.
   */
  @action
  toggleDeprecatedStatus(): void {
    this.toggleProperty('isDeprecatedAlias');
  }

  /**
   * Handles updates to the decommission time, triggered by our power calendar component through user
   * clicking on the date input
   * @param {Date} decommissionTime - date the entity should be decomissioned
   */
  @action
  onDecommissionDateChange(decommissionTime: Date): void {
    set(this, 'decommissionTimeAlias', new Date(decommissionTime).getTime());
  }

  /**
   * When a user clicks the checkbox to acknolwedge or cancel the acknolwedgement of the notice for
   * deprecating an entity
   */
  @action
  onAcknowledgeDeprecationNotice(): void {
    this.toggleProperty('isDeprecationAcknowledged');
  }
}
