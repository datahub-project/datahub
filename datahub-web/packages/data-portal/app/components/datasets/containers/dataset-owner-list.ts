import Component from '@ember/component';
import { set } from '@ember/object';
import { classNames } from '@ember-decorators/component';
import { computed, action } from '@ember/object';
import { task } from 'ember-concurrency';
import { readDatasetOwnersByUrn } from 'wherehows-web/utils/api/datasets/owners';
import { arrayMap, arrayPipe } from 'wherehows-web/utils/array';
import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { IOwner, IOwnerResponse } from 'wherehows-web/typings/api/datasets/owners';
import { makeAvatar } from 'wherehows-web/constants/avatars/avatars';
import {
  confirmedOwners,
  avatarWithDropDownOption,
  avatarWithProfileLink
} from 'wherehows-web/constants/datasets/owner';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { IAppConfig } from '@datahub/shared/types/configurator/configurator';
import { buildMailToUrl } from 'wherehows-web/utils/helpers/email';
import { INachoDropdownOption } from '@nacho-ui/dropdown/types/nacho-dropdown';

import { decodeUrn } from '@datahub/utils/validators/urn';
import { isLiUrn } from '@datahub/data-models/entity/dataset/utils/urn';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

@classNames('dataset-owner-list')
@containerDataSource('getOwnersTask', ['urn'])
export default class DatasetOwnerListContainer extends Component {
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
  owners: Array<IOwner> = [];

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
    const [makeAvatars, augmentAvatarsWithDropDownOption, augmentAvatarsWithProfileLink] = [
      arrayMap(makeAvatar(avatarEntityProps)),
      arrayMap(avatarWithDropDownOption),
      arrayMap(avatarWithProfileLink)
    ];
    const activeOwners = owners.filter((owner): boolean => owner.isActive);

    return arrayPipe(makeAvatars, augmentAvatarsWithDropDownOption, augmentAvatarsWithProfileLink)(activeOwners);
  }

  /**
   * Handles user selection of an option for each owner avatar
   * @param {IAvatar} avatar owner's avatar instance
   * @param {INachoDropdownOption<any>} [_option] unused optional parameter indicating selected option
   * @returns {(Window | null)}
   * @memberof DatasetOwnerListContainer
   */
  @action
  onOwnerOptionSelected(avatar: IAvatar, _option?: INachoDropdownOption<unknown>): Window | null {
    // if the owner avatar does not have an email then a null value is returned with no action performed
    const emailOwner = ({ email }: IAvatar): Window | null =>
      email ? window.open(buildMailToUrl({ to: email || '' }), '_blank') : null;

    return emailOwner(avatar);
  }

  /**
   * Reads the owners for this dataset
   */
  @(task(function*(this: DatasetOwnerListContainer): IterableIterator<Promise<IOwnerResponse>> {
    const { urn } = this;

    if (isLiUrn(decodeUrn(urn))) {
      const { owners = [] }: IOwnerResponse = yield readDatasetOwnersByUrn(urn);

      set(this, 'owners', confirmedOwners(owners));
    }
  }).restartable())
  getOwnersTask!: ETaskPromise<IOwnerResponse>;
}
