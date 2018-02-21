import Component from '@ember/component';
import { get, setProperties } from '@ember/object';
import { task } from 'ember-concurrency';
import { action } from 'ember-decorators/object';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { readDatasetByUrn } from 'wherehows-web/utils/api/datasets/dataset';
import { updateDatasetDeprecationByUrn } from 'wherehows-web/utils/api/datasets/properties';

export default class DatasetPropertiesContainer extends Component {
  /**
   * The urn identifier for the dataset
   * @type {string}
   */
  urn: string;

  /**
   * Flag indicating that the dataset is deprecated
   * @type {boolean | null}
   */
  deprecated: boolean | null;

  /**
   * Text string, intended to indicate the reason for deprecation
   * @type {string | null}
   */
  deprecationNote: string | null;

  /**
   * THe list of properties for the dataset, currently unavailable for v2
   * @type {any[]}
   */
  properties: Array<never> = [];

  constructor() {
    super(...arguments);
    this.deprecationNote || (this.deprecationNote = '');
  }

  didInsertElement() {
    get(this, 'getDeprecationPropertiesTask').perform();
  }

  didUpdateAttrs() {
    get(this, 'getDeprecationPropertiesTask').perform();
  }

  /**
   * Reads the persisted deprecation properties for the dataset
   * @type {Task<Promise<IDatasetView>, (a?: any) => TaskInstance<Promise<IDatasetView>>>}
   */
  getDeprecationPropertiesTask = task(function*(
    this: DatasetPropertiesContainer
  ): IterableIterator<Promise<IDatasetView>> {
    const { deprecated, deprecationNote } = yield readDatasetByUrn(get(this, 'urn'));
    setProperties(this, { deprecated, deprecationNote });
  });

  @action
  /**
   * Persists the changes to the dataset deprecation properties upstream
   * @param {boolean} isDeprecated
   * @param {string} updatedDeprecationNote
   * @return {Promise<void>}
   */
  async updateDeprecation(isDeprecated: boolean, updatedDeprecationNote: string): Promise<void> {
    await updateDatasetDeprecationByUrn(get(this, 'urn'), isDeprecated, updatedDeprecationNote);
    get(this, 'getDeprecationPropertiesTask').perform();
  }
}
