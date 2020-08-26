import Component from '@ember/component';
import { get } from '@ember/object';
import { task } from 'ember-concurrency';
import { isLiUrn } from '@datahub/data-models/entity/dataset/utils/urn';
import { DatasetOrigins } from 'datahub-web/typings/api/datasets/origins';
import { readDatasetOriginsByUrn } from 'datahub-web/utils/api/datasets/origins';
import { isArray } from '@ember/array';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

@containerDataSource<DatasetFabricsContainer>('getFabricsTask', ['urn'])
export default class DatasetFabricsContainer extends Component {
  /**
   * Urn string for the related dataset, supplied as an external attribute
   * @type {string}
   * @memberof DatasetFabricsContainer
   */
  urn!: string;

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
    if (isLiUrn(this.urn)) {
      const dataOrigins = ((yield readDatasetOriginsByUrn(this.urn)) as unknown) as DatasetOrigins;

      if (isArray(dataOrigins)) {
        get(this, 'fabrics').setObjects(dataOrigins);
      }
    }
  })
  getFabricsTask!: ETaskPromise<DatasetOrigins>;
}
