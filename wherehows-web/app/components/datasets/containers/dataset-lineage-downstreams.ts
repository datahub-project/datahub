import Component from '@ember/component';
import { get, set } from '@ember/object';
import { task } from 'ember-concurrency';
import { readDownstreamDatasetsByUrn } from 'wherehows-web/utils/api/datasets/lineage';
import { assert } from '@ember/debug';
import { IDatasetLineage } from 'wherehows-web/typings/api/datasets/relationships';

export default class DatasetLineageDownstreamsContainer extends Component {
  /**
   * Urn string for the related dataset, supplied as an external attribute
   * @type {string}
   * @memberof DatasetLineageDownstreamsContainer
   */
  urn!: string;

  /**
   * List of downstreams datasets for this urn
   * @type {Array<IDatasetLineage>}
   * @memberof DatasetLineageDownstreamsContainer
   */
  downstreams: Array<IDatasetLineage> = [];

  /**
   * Creates an instance of DatasetLineageDownstreamsContainer.
   * @memberof DatasetLineageDownstreamsContainer
   */
  constructor() {
    super(...arguments);

    const typeOfUrn = typeof this.urn;
    assert(`Expected prop urn to be of type string, got ${typeOfUrn}`, typeOfUrn === 'string');
  }

  didInsertElement() {
    get(this, 'getDatasetDownstreamsTask').perform();
  }

  didUpdateAttrs() {
    get(this, 'getDatasetDownstreamsTask').perform();
  }

  /**
   * Task to request and set dataset downstreams for this urn
   * @type {TaskProperty<Promise<Relationships>> & {perform: (a?: {} | undefined) => TaskInstance<Promise<Relationships>>}}
   * @memberof DatasetLineageDownstreamsContainer
   */
  getDatasetDownstreamsTask = task(function*(
    this: DatasetLineageDownstreamsContainer
  ): IterableIterator<Promise<Array<IDatasetLineage>>> {
    let downstreams: Array<IDatasetLineage> = [];

    try {
      downstreams = yield readDownstreamDatasetsByUrn(get(this, 'urn'));
    } finally {
      set(this, 'downstreams', downstreams);
    }
  });
}
