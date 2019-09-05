import Component from '@ember/component';
import { set } from '@ember/object';
import { task } from 'ember-concurrency';
import { readUpstreamDatasetsByUrn } from 'wherehows-web/utils/api/datasets/lineage';
import { LineageList } from 'wherehows-web/typings/api/datasets/relationships';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

@containerDataSource('getDatasetUpstreamsTask', ['urn'])
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
   * Task to request and set dataset upstreams for this urn
   * @type {TaskProperty<Promise<Relationships>> & {perform: (a?: {} | undefined) => TaskInstance<Promise<Relationships>>}}
   * @memberof DatasetLineageUpstreamsContainer
   */
  @task(function*(this: DatasetLineageUpstreamsContainer): IterableIterator<Promise<LineageList>> {
    let upstreams: LineageList = [];

    try {
      upstreams = yield readUpstreamDatasetsByUrn(this.urn);
    } finally {
      set(this, 'upstreams', upstreams);
    }
  })
  getDatasetUpstreamsTask!: ETaskPromise<LineageList>;
}
