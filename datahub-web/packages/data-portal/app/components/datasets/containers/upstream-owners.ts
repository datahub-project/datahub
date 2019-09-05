import Component from '@ember/component';
import { task } from 'ember-concurrency';
import { get, set } from '@ember/object';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { readDatasetByUrn } from 'wherehows-web/utils/api/datasets/dataset';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

@containerDataSource('getUpstreamPropertiesTask', ['upstreamUrn'])
export default class UpstreamOwners extends Component {
  /**
   * urn for the parent dataset
   * @type {string}
   * @memberof UpstreamOwners
   */
  upstreamUrn: string;

  /**
   * The name of the upstream dataset
   * @type {IDatasetView.nativeName}
   * @memberof UpstreamOwners
   */
  nativeName: IDatasetView['nativeName'];

  /**
   * Task to get properties for the upstream dataset
   * @memberof UpstreamOwners
   */
  @task(function*(this: UpstreamOwners): IterableIterator<Promise<IDatasetView>> {
    const { nativeName }: IDatasetView = yield readDatasetByUrn(get(this, 'upstreamUrn'));
    set(this, 'nativeName', nativeName);
  })
  getUpstreamPropertiesTask!: ETaskPromise<IDatasetView>;
}
