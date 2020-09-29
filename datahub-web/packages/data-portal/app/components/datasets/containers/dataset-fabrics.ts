import Component from '@ember/component';
import { set, computed } from '@ember/object';
import { task } from 'ember-concurrency';
import { isLiUrn } from '@datahub/data-models/entity/dataset/utils/urn';
import { DatasetOrigins } from 'datahub-web/typings/api/datasets/origins';
import { readDatasetOriginsByUrn } from 'datahub-web/utils/api/datasets/origins';
import { isArray } from '@ember/array';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

@containerDataSource<DatasetFabricsContainer>('getFabricsTask', ['entity'])
export default class DatasetFabricsContainer extends Component {
  /**
   * The Dataset which provides context to this container as to what entity we are dealing with
   */
  entity?: DatasetEntity;

  /**
   * Urn string for the related dataset, supplied as an external attribute
   */
  @computed('entity')
  get urn(): string {
    return this.entity?.urn || '';
  }

  /**
   * Lists the Fabrics for the dataset with this urn
   * @type {DatasetOrigins}
   * @memberof DatasetFabricsContainer
   */
  fabrics: DatasetOrigins = [];

  /**
   * Reads the fabrics available for the dataset with this urn and sets the value of
   * the related list of available Fabrics
   */
  @task(function*(this: DatasetFabricsContainer): IterableIterator<Promise<DatasetOrigins>> {
    const { urn } = this;

    if (isLiUrn(urn)) {
      const dataOrigins = ((yield readDatasetOriginsByUrn(urn)) as unknown) as DatasetOrigins;

      if (isArray(dataOrigins)) {
        set(this, 'fabrics', dataOrigins);
      }
    }
  })
  getFabricsTask!: ETaskPromise<DatasetOrigins>;
}
