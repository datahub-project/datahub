import Component from '@ember/component';
import { task } from 'ember-concurrency';
import { IDatasetHealth } from 'wherehows-web/typings/api/datasets/health';
import { readDatasetHealthByUrn } from 'wherehows-web/utils/api/datasets/health';
import { get, set } from '@ember/object';

export default class HealthScoreGauge extends Component {
  /**
   * The urn identifier for the dataset
   * @type {string}
   */
  urn: string;

  /**
   * Defines the tag to be used in the rendered html element for this component
   * @type {string}
   */
  tagName = '';

  elementId = <any>undefined;

  /**
   * The health score to be passed to the container
   */
  healthScore: number = 0;

  didInsertElement() {
    get(this, 'getHealthScoreTask').perform();
  }

  /**
   * An async parent task to group all data tasks for this container component
   * @type {Task<TaskInstance<Promise<any>>, (a?: any) => TaskInstance<TaskInstance<Promise<any>>>>}
   */
  getHealthScoreTask = task(function*(this: HealthScoreGauge): IterableIterator<Promise<IDatasetHealth>> {
    const health: IDatasetHealth = yield readDatasetHealthByUrn(get(this, 'urn'));
    set(this, 'healthScore', health.score * 100 || 0);
  }).restartable();
}
