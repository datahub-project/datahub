import Component from '@ember/component';
import { set } from '@ember/object';
import { task } from 'ember-concurrency';
import { readDownstreamDatasetsByUrn } from 'datahub-web/utils/api/datasets/lineage';
import { DatasetLineageList } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { IDatasetLineage } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { map } from '@ember/object/computed';

@containerDataSource<DatasetLineageDownstreamsContainer>('getDatasetDownstreamsTask', ['urn'])
export default class DatasetLineageDownstreamsContainer extends Component {
  /**
   * Urn string for the related dataset, supplied as an external attribute
   * @type {string}
   * @memberof DatasetLineageDownstreamsContainer
   */
  urn!: string;

  /**
   * List of downstreams datasets for this urn
   * @memberof DatasetLineageDownstreamsContainer
   */
  downstreams: DatasetLineageList = [];

  /**
   * A map returning the dataset urns from the downstreams list
   */
  @map('downstreams', (downstream: IDatasetLineage): string => downstream.dataset.uri)
  downstreamUrns!: Array<string>;

  /**
   * Task to request and set dataset downstreams for this urn
   * @type {TaskProperty<Promise<Relationships>> & {perform: (a?: {} | undefined) => TaskInstance<Promise<Relationships>>}}
   * @memberof DatasetLineageDownstreamsContainer
   */
  @task(function*(this: DatasetLineageDownstreamsContainer): IterableIterator<Promise<DatasetLineageList>> {
    let downstreams: DatasetLineageList = [];

    try {
      downstreams = ((yield readDownstreamDatasetsByUrn(this.urn)) as unknown) as DatasetLineageList;
    } finally {
      set(this, 'downstreams', downstreams);
    }
  })
  getDatasetDownstreamsTask!: ETaskPromise<DatasetLineageList>;
}
