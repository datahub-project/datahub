import Component from '@ember/component';
import { task } from 'ember-concurrency';
import { get, set } from '@ember/object';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { assert } from '@ember/debug';
import { readDatasetByUrn } from 'wherehows-web/utils/api/datasets/dataset';

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

  constructor() {
    super(...arguments);

    assert('A valid upstreamUrn must be provided on instantiation', typeof this.upstreamUrn === 'string');
  }

  didUpdateAttrs() {
    this._super(...arguments);
    get(this, 'getUpstreamPropertiesTask').perform();
  }

  didInsertElement() {
    this._super(...arguments);
    get(this, 'getUpstreamPropertiesTask').perform();
  }

  /**
   * Task to get properties for the upstream dataset
   * @type {Task<Promise<IDatasetView>>, (a?: {} | undefined) => TaskInstance<Promise<IDatasetView>>>}
   * @memberof UpstreamOwners
   */
  getUpstreamPropertiesTask = task(function*(this: UpstreamOwners): IterableIterator<Promise<IDatasetView>> {
    const { nativeName }: IDatasetView = yield readDatasetByUrn(get(this, 'upstreamUrn'));
    set(this, 'nativeName', nativeName);
  });
}
