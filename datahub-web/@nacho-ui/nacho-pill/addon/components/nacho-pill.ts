import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/nacho-pill';
import { layout, className, classNames } from '@ember-decorators/component';
import { computed } from '@ember/object';

/**
 * These are the predefined states that we provide for nacho pills. Based on how we layout this component, consumers
 * can also define their own states and add the relevant CSS classes accordingly
 */
export enum PillState {
  Neutral = 'neutral',
  Alert = 'alert',
  Warning = 'warning',
  Action = 'action',
  Good = 'good',
  GoodInverse = 'good-inverse',
  NeutralInverse = 'neutral-inverse'
}

/**
 * Pills are self-contained shapes that can have a range of interaction types and are visually styled to differentiate them
 * from buttons. They can be added, removed, toggled on and off or defined by content inputted by the user within the interface.
 * They can also be used as tags in order to display certain states of information for other components they are placed around
 *
 * @example
 * {{nacho-pill text="hello" state="neutral"}}
 */
@layout(template)
@classNames('nacho-pill')
export default class NachoPill extends Component {
  /**
   * Determines the coloring scheme of the pill by applying various UI states to it
   * @type {PillState}
   */
  state!: PillState;

  /**
   * tooltip message if needed for a tooltip
   */
  tooltip?: string;

  /**
   * Determines the CSS classes that must be applied to the pill based on the determination of
   * various properties passed into this component
   * @type {string}
   */
  @className
  @computed('state')
  get pillClasses(): string {
    return `nacho-pill--${this.state}`;
  }

  constructor() {
    // eslint-disable-next-line prefer-rest-params
    super(...arguments);
    // Applying default here as the version of ember of this package is not high enough yet to be able
    // to assign default properties within the class field declaration itself
    typeof this.state === 'string' || (this.state = PillState.Neutral);
  }
}
