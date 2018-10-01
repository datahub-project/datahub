import Component from '@ember/component';
import { set } from '@ember/object';
import { classNames } from '@ember-decorators/component';
import { computed, action } from '@ember-decorators/object';
import { assert } from '@ember/debug';
import { task } from 'ember-concurrency';
import { readDatasetOwnersByUrn } from 'wherehows-web/utils/api/datasets/owners';
import { arrayMap, arrayPipe } from 'wherehows-web/utils/array';
import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { IOwner, IOwnerResponse } from 'wherehows-web/typings/api/datasets/owners';
import { makeAvatar } from 'wherehows-web/constants/avatars/avatars';
import { confirmedOwners, avatarWithDropDownOption } from 'wherehows-web/constants/datasets/owner';
import { containerDataSource } from 'wherehows-web/utils/components/containers/data-source';
import { decodeUrn, isLiUrn } from 'wherehows-web/utils/validators/urn';
import { IAppConfig } from 'wherehows-web/typings/api/configurator/configurator';
import { buildMailToUrl } from 'wherehows-web/utils/helpers/email';
import { IDropDownOption } from 'wherehows-web/typings/app/dataset-compliance';

@classNames('dataset-owner-list')
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
  avatarEntityProps!: IAppConfig['userEntityProps'];

  /**
   * Lists the avatar objects based off the dataset owners
   * @type {ComputedProperty<Array<IAvatar>>}
   * @memberof DatasetOwnerListContainer
   */
  @computed('owners')
  get avatars(): Array<IAvatar> {
    const { avatarEntityProps, owners } = this;
    const [makeAvatars, augmentAvatarsWithDropDownOption] = [
      arrayMap(makeAvatar(avatarEntityProps)),
      arrayMap(avatarWithDropDownOption)
    ];

    return arrayPipe(makeAvatars, augmentAvatarsWithDropDownOption)(owners);
  }

  /**
   * Handles user selection of an option for each owner avatar
   * @param {IAvatar} avatar owner's avatar instance
   * @param {IDropDownOption<any>} [_option] unused optional parameter indicating selected option
   * @returns {(Window | null)}
   * @memberof DatasetOwnerListContainer
   */
  @action
  onOwnerOptionSelected(avatar: IAvatar, _option?: IDropDownOption<any>): Window | null {
    // if the owner avatar does not have an email then a null value is returned with no action performed
    const emailOwner = ({ email }: IAvatar): Window | null =>
      email ? window.open(buildMailToUrl({ to: email || '' }), '_blank') : null;

    return emailOwner(avatar);
  }

  /**
   * Reads the owners for this dataset
   * @type {Task<Promise<Array<IOwnerResponse>>, () => TaskInstance<Promise<IOwnerResponse>>>}
   */
  getOwnersTask = task(function*(this: DatasetOwnerListContainer): IterableIterator<Promise<IOwnerResponse>> {
    const { urn } = this;

    if (isLiUrn(decodeUrn(urn))) {
      const { owners = [] }: IOwnerResponse = yield readDatasetOwnersByUrn(urn);

      set(this, 'owners', confirmedOwners(owners));
    }
  }).restartable();
}
