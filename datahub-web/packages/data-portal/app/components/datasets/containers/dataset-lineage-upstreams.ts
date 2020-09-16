import Component from '@ember/component';
import { set } from '@ember/object';
import { task } from 'ember-concurrency';
import { readUpstreamDatasetsByUrn } from 'datahub-web/utils/api/datasets/lineage';
import { DatasetLineageList } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { IDatasetLineage } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { map } from '@ember/object/computed';

@containerDataSource<DatasetLineageUpstreamsContainer>('getDatasetUpstreamsTask', ['urn'])
export default class DatasetLineageUpstreamsContainer extends Component {
  /**
   * Urn string for the related dataset, supplied as an external attribute
   * @type {string}
   * @memberof DatasetLineageUpstreamsContainer
   */
  urn!: string;

  /**
   * List of upstream datasets for this urn
   * @memberof DatasetLineageUpstreamsContainer
   */
  upstreams: DatasetLineageList = [];

  /**
   * A map returning the dataset urns from the upstreams list
   */
  @map('upstreams', (upstream: IDatasetLineage): string => upstream.dataset.uri)
  upstreamUrns!: Array<string>;

  /**
   * Task to request and set dataset upstreams for this urn
   * @type {TaskProperty<Promise<Relationships>> & {perform: (a?: {} | undefined) => TaskInstance<Promise<Relationships>>}}
   * @memberof DatasetLineageUpstreamsContainer
   */
  @task(function*(this: DatasetLineageUpstreamsContainer): IterableIterator<Promise<DatasetLineageList>> {
    let upstreams: DatasetLineageList = [];

    try {
      upstreams = ((yield readUpstreamDatasetsByUrn(this.urn)) as unknown) as DatasetLineageList;
    } finally {
      set(this, 'upstreams', upstreams);
    }
  })
  getDatasetUpstreamsTask!: ETaskPromise<DatasetLineageList>;
}
