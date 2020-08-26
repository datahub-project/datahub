import Component from '@ember/component';
import { task } from 'ember-concurrency';
import { get, set } from '@ember/object';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { readDataset } from '@datahub/data-models/api/dataset/dataset';

@containerDataSource<UpstreamOwners>('getUpstreamPropertiesTask', ['upstreamUrn'])
export default class UpstreamOwners extends Component {
  /**
   * urn for the parent dataset
   * @type {string}
   * @memberof UpstreamOwners
   */
  upstreamUrn: string;

  /**
   * The name of the upstream dataset
   * @type {IDatasetEntity.nativeName}
   * @memberof UpstreamOwners
   */
  nativeName: Com.Linkedin.Dataset.Dataset['name'];

  /**
   * Task to get properties for the upstream dataset
   * @memberof UpstreamOwners
   */
  @task(function*(this: UpstreamOwners): IterableIterator<Promise<Com.Linkedin.Dataset.Dataset>> {
    const { name } = ((yield readDataset(get(this, 'upstreamUrn'))) as unknown) as Com.Linkedin.Dataset.Dataset;
    set(this, 'nativeName', name);
  })
  getUpstreamPropertiesTask!: ETaskPromise<Com.Linkedin.Dataset.Dataset>;
}
