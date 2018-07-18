import Component from '@ember/component';
import { get } from '@ember/object';
import { task } from 'ember-concurrency';
import { Fabric } from 'wherehows-web/constants';
import { readDatasetFabricsByUrn } from 'wherehows-web/utils/api/datasets/fabrics';

export default class DatasetFabricsContainer extends Component {
  /**
   * Urn string for the related dataset, supplied as an external attribute
   * @type {string}
   * @memberof DatasetFabricsContainer
   */
  urn: string;

  /**
   * Lists the Fabrics for the dataset with this urn
   * @type {Array<Fabric>}
   * @memberof DatasetFabricsContainer
   */
  fabrics: Array<Fabric>;

  /**
   * Creates an instance of DatasetFabricsContainer.
   * @memberof DatasetFabricsContainer
   */
  constructor() {
    super(...arguments);

    this.fabrics || (this.fabrics = []);
  }

  didInsertElement() {
    get(this, 'getFabricsTask').perform();
  }

  didUpdateAttrs() {
    get(this, 'getFabricsTask').perform();
  }

  /**
   * Reads the fabrics available for the dataset with this urn and sets the value of
   * the related list of available Fabrics
   * @type {Task<Promise<Array<Fabric>>, (a?: any) => TaskInstance<Promise<Array<Fabric>>>>}
   */
  getFabricsTask = task(function*(this: DatasetFabricsContainer): IterableIterator<Promise<Array<Fabric>>> {
    const fabrics: Array<Fabric> = yield readDatasetFabricsByUrn(get(this, 'urn'));

    if (Array.isArray(fabrics)) {
      get(this, 'fabrics').setObjects(fabrics);
    }
  });
}
