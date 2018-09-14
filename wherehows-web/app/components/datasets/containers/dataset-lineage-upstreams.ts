import Component from '@ember/component';
import { get, set } from '@ember/object';
import { task } from 'ember-concurrency';
import { readUpstreamDatasetsByUrn } from 'wherehows-web/utils/api/datasets/lineage';
import { assert } from '@ember/debug';
import { LineageList } from 'wherehows-web/typings/api/datasets/relationships';

export default class DatasetLineageUpstreamsContainer extends Component {
  /**
   * Urn string for the related dataset, supplied as an external attribute
   * @type {string}
   * @memberof DatasetLineageUpstreamsContainer
   */
  urn!: string;

  /**
   * List of upstream datasets for this urn
   * @type {LineageList}
   * @memberof DatasetLineageUpstreamsContainer
   */
  upstreams: LineageList = [];

  /**
   * Creates an instance of DatasetLineageUpstreamsContainer.
   * @memberof DatasetLineageUpstreamsContainer
   */
  constructor() {
    super(...arguments);

    const typeOfUrn = typeof this.urn;
    assert(`Expected prop urn to be of type string, got ${typeOfUrn}`, typeOfUrn === 'string');
  }

  didInsertElement() {
    get(this, 'getDatasetUpstreamsTask').perform();
  }

  didUpdateAttrs() {
    get(this, 'getDatasetUpstreamsTask').perform();
  }

  /**
   * Task to request and set dataset upstreams for this urn
   * @type {TaskProperty<Promise<Relationships>> & {perform: (a?: {} | undefined) => TaskInstance<Promise<Relationships>>}}
   * @memberof DatasetLineageUpstreamsContainer
   */
  getDatasetUpstreamsTask = task(function*(
    this: DatasetLineageUpstreamsContainer
  ): IterableIterator<Promise<LineageList>> {
    let upstreams: LineageList = [];

    try {
      upstreams = yield readUpstreamDatasetsByUrn(get(this, 'urn'));
    } finally {
      set(this, 'upstreams', upstreams);
    }
  });
}
