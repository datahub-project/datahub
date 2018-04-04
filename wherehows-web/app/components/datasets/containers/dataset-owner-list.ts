import Component from '@ember/component';
import { get, set, computed } from '@ember/object';
import ComputedProperty from '@ember/object/computed';
import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { IOwner, IOwnerResponse } from 'wherehows-web/typings/api/datasets/owners';
import { task } from 'ember-concurrency';
import { readDatasetOwnersByUrn } from 'wherehows-web/utils/api/datasets/owners';
import { arrayMap } from 'wherehows-web/utils/array';
import { getAvatarProps } from 'wherehows-web/constants/avatars/avatars';
import { confirmedOwners } from 'wherehows-web/constants/datasets/owner';

export default class DatasetOwnerListContainer extends Component {
  constructor() {
    super(...arguments);

    this.owners || (this.owners = []);
  }

  /**
   * Urn for the related dataset
   * @type {string}
   * @memberof DatasetOwnerListContainer
   */
  urn: string;

  /**
   * The owners for the dataset
   * @type {Array<IOwner>}
   * @memberof DatasetOwnerListContainer
   */
  owners: Array<IOwner>;

  /**
   * Lists the avatar objects based off the dataset owners
   * @type {ComputedProperty<Array<IAvatar>>}
   * @memberof DatasetOwnerListContainer
   */
  avatars: ComputedProperty<Array<IAvatar>> = computed('owners', function(): Array<IAvatar> {
    return arrayMap(getAvatarProps)(get(this, 'owners'));
  });

  didInsertElement() {
    get(this, 'getOwnersTask').perform();
  }

  didUpdateAttrs() {
    get(this, 'getOwnersTask').perform();
  }

  /**
   * Reads the owners for this dataset
   * @type {Task<Promise<Array<IOwnerResponse>>, () => TaskInstance<Promise<IOwnerResponse>>>}
   */
  getOwnersTask = task(function*(this: DatasetOwnerListContainer): IterableIterator<Promise<IOwnerResponse>> {
    const { owners = [] }: IOwnerResponse = yield readDatasetOwnersByUrn(get(this, 'urn'));

    set(this, 'owners', confirmedOwners(owners));
  }).restartable();
}
