import Component from '@ember/component';
import { set } from '@ember/object';
import { task } from 'ember-concurrency';
import { assert } from '@ember/debug';
import { computed } from '@ember-decorators/object';
import { readDatasetOwnersByUrn } from 'wherehows-web/utils/api/datasets/owners';
import { arrayMap } from 'wherehows-web/utils/array';
import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { IOwner, IOwnerResponse } from 'wherehows-web/typings/api/datasets/owners';
import { getAvatarProps } from 'wherehows-web/constants/avatars/avatars';
import { confirmedOwners } from 'wherehows-web/constants/datasets/owner';
import { containerDataSource } from 'wherehows-web/utils/components/containers/data-source';
import { isLiUrn } from 'wherehows-web/utils/validators/urn';
import { IAppConfig } from 'wherehows-web/typings/api/configurator/configurator';

@containerDataSource('getOwnersTask')
export default class DatasetOwnerListContainer extends Component {
  constructor() {
    super(...arguments);

    const typeOfAvatarEntityProps = typeof this.avatarEntityProps;

    assert(
      `Expected avatarEntityProps to be an object, got ${typeOfAvatarEntityProps}`,
      typeOfAvatarEntityProps === 'object'
    );

    Array.isArray(this.owners) || (this.owners = []);
  }

  /**
   * Urn for the related dataset
   * @type {string}
   * @memberof DatasetOwnerListContainer
   */
  urn!: string;

  /**
   * The owners for the dataset
   * @type {Array<IOwner>}
   * @memberof DatasetOwnerListContainer
   */
  owners: Array<IOwner>;

  /**
   * Avatar entity properties used to constructs the avatar image
   * @type {IAppConfig.avatarEntityProps}
   * @memberof DatasetOwnerListContainer
   */
  avatarEntityProps!: IAppConfig['avatarEntityProps'];

  /**
   * Lists the avatar objects based off the dataset owners
   * @type {ComputedProperty<Array<IAvatar>>}
   * @memberof DatasetOwnerListContainer
   */
  @computed('owners')
  get avatars(): Array<IAvatar> {
    const { avatarEntityProps, owners } = this;
    return arrayMap(getAvatarProps(avatarEntityProps))(owners);
  }

  /**
   * Reads the owners for this dataset
   * @type {Task<Promise<Array<IOwnerResponse>>, () => TaskInstance<Promise<IOwnerResponse>>>}
   */
  getOwnersTask = task(function*(this: DatasetOwnerListContainer): IterableIterator<Promise<IOwnerResponse>> {
    const { urn } = this;

    if (isLiUrn(urn)) {
      const { owners = [] }: IOwnerResponse = yield readDatasetOwnersByUrn(urn);

      set(this, 'owners', confirmedOwners(owners));
    }
  }).restartable();
}
