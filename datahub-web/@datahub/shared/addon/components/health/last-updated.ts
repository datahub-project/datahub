import Component from '@glimmer/component';
import HealthProxy from '@datahub/shared/utils/health/health-proxy';
import { task, timeout } from 'ember-concurrency';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { set } from '@ember/object';
import { tracked } from '@glimmer/tracking';
import { action } from '@ember/object';
import moment from 'moment';

interface IHealthLastUpdatedArgs {
  // Reference to the Health metadata for the related entity if available
  lastUpdated?: HealthProxy['lastUpdated'];
  // Optional attribute indicating that the component should be rendered in a modified manner for a smaller screen area
  small?: boolean;
}

// Refresh delay for updating the last calculated text, 1 minute
export const advanceAfterMs = 1000 * 60;

/**
 * Presentational component to render the health score's previous recalculation time
 * in a humanized format as well as ensure the time is always rendered accurately from
 * the current time i.e. the value is updated every minute while the user is viewing this
 * component
 * -> a few seconds ago
 * -> a minute ago
 * -> 2 minutes ago
 * -> 3 minutes ago...
 * @export
 * @class HealthLastUpdated
 * @extends {Component<IHealthLastUpdatedArgs>}
 */
export default class HealthLastUpdated extends Component<IHealthLastUpdatedArgs> {
  /**
   *  Relative time from the current time e.g. a few seconds ago, 1 minute ago
   */
  @tracked
  timeFromNow = '';

  /**
   * Some context require the component to be rendered in a relatively different sized screen area
   * currently, smaller, this will produce a class modifier if the small argument is truthy
   * @readonly
   */
  get classModifier(): string {
    return this.args.small ? 'small' : '';
  }

  /**
   * Sets and recursively updates the timeFromNow value every advanceAfterMs (1 minute) so it's always accurate to the minute
   */
  @(task(function*(this: HealthLastUpdated): IterableIterator<Promise<void>> {
    const { lastUpdated } = this.args;

    if (lastUpdated) {
      set(this, 'timeFromNow', moment(lastUpdated).fromNow());
      yield timeout(advanceAfterMs);

      this.renderAtIntervalTask.perform();
    }
  }).enqueue())
  renderAtIntervalTask!: ETaskPromise<Promise<void>>;

  /**
   * Invoked by template render modifiers when the component is either inserted in the dom or args are updated
   * kicks off the interval task to refresh the relative timestamp
   */
  @action
  updateRelativeTimeAtInterval(): void {
    if (this.renderAtIntervalTask.last) {
      this.renderAtIntervalTask.last.cancel();
    }

    this.renderAtIntervalTask.perform();
  }
}
