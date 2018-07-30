import Component from '@ember/component';
import { get, set } from '@ember/object';
import { task } from 'ember-concurrency';
import { getUrnParts, isLiUrn } from 'wherehows-web/utils/validators/urn';
import { readDatasetsCount } from 'wherehows-web/utils/api/datasets/dataset';

export default class DataSystemsCountContainer extends Component {
  /**
   * The data system string urn
   * @type {string}
   * @memberof DataSystemsCountContainer
   */
  urn: string;

  /**
   * The count of datasets within the data system
   * @type {number|void}
   * @memberof DataSystemsCountContainer
   */
  count: number | void;

  didInsertElement() {
    get(this, 'getDataSystemCountTask').perform();
  }

  didUpdateAttrs() {
    get(this, 'getDataSystemCountTask').perform();
  }

  /**
   * EC task to request data system count
   * @type {(ComputedProperty<TaskProperty<Promise<number>> & {
    perform: (a?: {} | undefined) => TaskInstance<Promise<number>>}>)}
   * @memberof DataSystemsCountContainer
   */
  getDataSystemCountTask = task(function*(this: DataSystemsCountContainer): IterableIterator<Promise<number>> {
    const urn = get(this, 'urn');

    if (isLiUrn(urn)) {
      const { platform = '', prefix = '' } = getUrnParts(urn);
      const count = yield readDatasetsCount({ platform, prefix });

      set(this, 'count', count);
    }
  });
}
