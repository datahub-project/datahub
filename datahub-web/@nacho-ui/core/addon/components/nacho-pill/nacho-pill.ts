import Component from '@glimmer/component';
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
 * Base css class
 */
export const nachoPillBaseClass = 'nacho-pill';

export interface INachoPillArgs {
  /**
   * Additional css classes
   */
  class?: string;
  /**
   * Determines the coloring scheme of the pill by applying various UI states to it
   * @type {PillState}
   */
  state?: PillState;

  /**
   * tooltip message if needed for a tooltip
   */
  tooltip?: string;

  /**
   * Optional text if not using a block
   */
  text?: string;
}

/**
 * Pills are self-contained shapes that can have a range of interaction types and are visually styled to differentiate them
 * from buttons. They can be added, removed, toggled on and off or defined by content inputted by the user within the interface.
 * They can also be used as tags in order to display certain states of information for other components they are placed around
 *
 * @example
 * {{nacho-pill text="hello" state="neutral"}}
 */
export default class NachoPill<Args extends INachoPillArgs = INachoPillArgs> extends Component<Args> {
  /**
   * Base css class
   */
  nachoPillBaseClass = nachoPillBaseClass;

  /**
   * Determines the CSS classes that must be applied to the pill based on the determination of
   * various properties passed into this component
   * @type {string}
   */
  @computed('args.state')
  get pillClasses(): string {
    const { state = PillState.Neutral } = this.args;
    return `nacho-pill--${state}`;
  }
}
