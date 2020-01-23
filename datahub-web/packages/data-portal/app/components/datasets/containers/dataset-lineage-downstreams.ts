import Component from '@ember/component';
import { set } from '@ember/object';
import { task } from 'ember-concurrency';
import { readDownstreamDatasetsByUrn } from 'wherehows-web/utils/api/datasets/lineage';
import { LineageList } from 'wherehows-web/typings/api/datasets/relationships';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

@containerDataSource('getDatasetDownstreamsTask', ['urn'])
export default class DatasetLineageDownstreamsContainer extends Component {
  /**
   * Urn string for the related dataset, supplied as an external attribute
   * @type {string}
   * @memberof DatasetLineageDownstreamsContainer
   */
  urn!: string;

  /**
   * List of downstreams datasets for this urn
   * @type {LineageList}
   * @memberof DatasetLineageDownstreamsContainer
   */
  downstreams: LineageList = [];

  /**
   * Task to request and set dataset downstreams for this urn
   * @type {TaskProperty<Promise<Relationships>> & {perform: (a?: {} | undefined) => TaskInstance<Promise<Relationships>>}}
   * @memberof DatasetLineageDownstreamsContainer
   */
  @task(function*(this: DatasetLineageDownstreamsContainer): IterableIterator<Promise<LineageList>> {
    let downstreams: LineageList = [];

    try {
      downstreams = yield readDownstreamDatasetsByUrn(this.urn);
    } finally {
      set(this, 'downstreams', downstreams);
    }
  })
  getDatasetDownstreamsTask!: ETaskPromise<LineageList>;
}
